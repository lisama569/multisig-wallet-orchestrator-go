package multisig

import (
	"testing"
	"time"
)

// newTestOrchestrator 在临时目录构造一个阈值为 2/3、TTL 可控的钱包。
func newTestOrchestrator(t *testing.T, clock func() time.Time, balance int64) (*Orchestrator, *Policy) {
	t.Helper()
	o, err := NewOrchestrator(t.TempDir())
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	p, err := NewPolicy("w1", []string{"alice", "bob", "carol"}, 2, time.Hour)
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	o.AddPolicy(p)
	if clock != nil {
		o.SetClock(clock)
	}
	if balance > 0 {
		if err := o.Deposit("w1", balance); err != nil {
			t.Fatalf("Deposit: %v", err)
		}
	}
	return o, p
}

func buildReadyTx(t *testing.T, o *Orchestrator, key string, amount int64) *Transaction {
	t.Helper()
	tx, err := o.BuildTx(BuildRequest{WalletID: "w1", IdempotencyKey: key, To: "shop", Amount: amount})
	if err != nil {
		t.Fatalf("BuildTx: %v", err)
	}
	return tx
}

func TestApprovalRejectionReasons(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 1000)
	tx := buildReadyTx(t, o, "k1", 100)

	// 1) 非签名方越权审批
	_, err := o.Approve(tx.ID, "mallory")
	if code := ErrorCodeOf(err); code != ErrCodeUnauthorizedApprover {
		t.Fatalf("unauthorized approver: code=%s err=%v", code, err)
	}
	t.Logf("[input tx=%s approver=mallory] -> reject code=%s reason=%q", tx.ID, ErrorCodeOf(err), err)

	// 2) 正常审批
	if _, err := o.Approve(tx.ID, "alice"); err != nil {
		t.Fatalf("Approve alice: %v", err)
	}
	// 3) 同一签名方重复审批
	_, err = o.Approve(tx.ID, "alice")
	if code := ErrorCodeOf(err); code != ErrCodeDuplicateApproval {
		t.Fatalf("duplicate approval: code=%s err=%v", code, err)
	}
	t.Logf("[input tx=%s approver=alice repeated] -> reject code=%s reason=%q",
		tx.ID, ErrorCodeOf(err), err)

	// 4) 只有 1 个审批时提交 -> 阈值不足
	_, err = o.Submit(tx.ID)
	if code := ErrorCodeOf(err); code != ErrCodePolicyNotSatisfied {
		t.Fatalf("policy not satisfied: code=%s err=%v", code, err)
	}
	t.Logf("[input tx=%s approvals=1 threshold=2] -> reject code=%s reason=%q",
		tx.ID, ErrorCodeOf(err), err)
}

func TestExpiryRejectsApprovalAndSubmit(t *testing.T) {
	base := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	now := base
	o, _ := newTestOrchestrator(t, func() time.Time { return now }, 1000)

	tx := buildReadyTx(t, o, "k-exp", 100)
	now = base.Add(61 * time.Minute) // 超过 1h TTL

	if _, err := o.Approve(tx.ID, "alice"); ErrorCodeOf(err) != ErrCodeExpired {
		t.Fatalf("expired approval: %v", err)
	}
	t.Logf("[input tx=%s now=%s expiresAt=%s] -> reject code=%s",
		tx.ID, now.Format(time.RFC3339), tx.ExpiresAt.Format(time.RFC3339), ErrCodeExpired)

	got, _ := o.Get(tx.ID)
	if got.Status != StatusExpired {
		t.Fatalf("status = %s, want expired", got.Status)
	}
	// 终态上再提交 -> 过期错误，而不是被容忍
	if _, err := o.Submit(tx.ID); ErrorCodeOf(err) != ErrCodeExpired {
		t.Fatalf("submit on expired tx: %v", err)
	}
}

func TestInvalidTransitionsRejected(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 1000)
	tx := buildReadyTx(t, o, "k-done", 100)
	if _, err := o.Approve(tx.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Approve(tx.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Submit(tx.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// executed 终态上审批 -> 非法跃迁
	if _, err := o.Approve(tx.ID, "carol"); ErrorCodeOf(err) != ErrCodeInvalidTransition {
		t.Fatalf("approve after execute: %v", err)
	}
	t.Logf("[input tx=%s status=executed action=approve] -> reject code=%s",
		tx.ID, ErrCodeInvalidTransition)

	// 对非 failed 交易 Retry -> 非法跃迁
	if _, err := o.Retry(tx.ID); ErrorCodeOf(err) != ErrCodeInvalidTransition {
		t.Fatalf("retry executed: %v", err)
	}
}

func TestInsufficientBalance(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 50)
	tx := buildReadyTx(t, o, "k-poor", 100)
	if _, err := o.Approve(tx.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Approve(tx.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	_, err := o.Submit(tx.ID)
	if code := ErrorCodeOf(err); code != ErrCodeInsufficientBalance {
		t.Fatalf("insufficient balance: code=%s err=%v", code, err)
	}
	t.Logf("[input wallet=w1 balance=50 amount=100] -> reject code=%s reason=%q", ErrorCodeOf(err), err)

	got, _ := o.Get(tx.ID)
	if got.Status != StatusFailed || got.FailureCode != ErrCodeInsufficientBalance {
		t.Fatalf("failed tx status=%s failureCode=%s", got.Status, got.FailureCode)
	}
	// 充值后 Retry + 再提交应成功
	if err := o.Deposit("w1", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Retry(tx.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	r, err := o.Submit(tx.ID)
	if err != nil {
		t.Fatalf("Submit after retry: %v", err)
	}
	if r.Status != StatusExecuted || o.Balance("w1") != 50 {
		t.Fatalf("unexpected: receipt=%+v balance=%d", r, o.Balance("w1"))
	}
	t.Logf("[recovery] deposit then retry -> executed, balance=%d", o.Balance("w1"))
}
