package multisig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func registerDefaultPolicy(t *testing.T, o *Orchestrator) {
	t.Helper()
	p, err := NewPolicy("w1", []string{"alice", "bob", "carol"}, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	o.AddPolicy(p)
}

func setupWalletDir(t *testing.T, dir string, balance int64) {
	t.Helper()
	o, err := NewOrchestrator(dir)
	if err != nil {
		t.Fatal(err)
	}
	registerDefaultPolicy(t, o)
	if err := o.Deposit("w1", balance); err != nil {
		t.Fatal(err)
	}
}

func TestReloadAndReplayNoDoubleDebit(t *testing.T) {
	dir := t.TempDir()
	setupWalletDir(t, dir, 1000)

	// 第一个进程：构建、审批、提交并扣款。
	o1, err := NewOrchestrator(dir)
	if err != nil {
		t.Fatal(err)
	}
	registerDefaultPolicy(t, o1)
	tx, err := o1.BuildTx(BuildRequest{WalletID: "w1", IdempotencyKey: "k-rel", To: "shop", Amount: 300})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Approve(tx.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := o1.Approve(tx.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	r1, err := o1.Submit(tx.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[process 1] executed tx=%s nonce=%d balance=%d", tx.ID, r1.Nonce, o1.Balance("w1"))

	// 第二个进程：从磁盘重载后重复提交 -> 必须命中账本去重，不重复扣款。
	o2, err := NewOrchestrator(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := o2.Get(tx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusExecuted {
		t.Fatalf("reloaded status=%s want executed", got.Status)
	}
	r2, err := o2.Submit(tx.ID)
	if err != nil {
		t.Fatalf("replay after reload: %v", err)
	}
	if !r2.Replayed || r2.Nonce != 0 {
		t.Fatalf("reload replay must be idempotent: %+v", r2)
	}
	if bal := o2.Balance("w1"); bal != 700 {
		t.Fatalf("balance after reload replay=%d want 700", bal)
	}
	// 重载后新交易必须从 nonce=1 开始，游标不能倒退或空洞。
	nxt, err := o2.BuildTx(BuildRequest{WalletID: "w1", IdempotencyKey: "k-rel-2", To: "shop", Amount: 10})
	if err != nil {
		t.Fatal(err)
	}
	if nxt.Nonce != 1 {
		t.Fatalf("nonce after reload=%d want 1", nxt.Nonce)
	}
	t.Logf("[process 2 reloaded] replay -> replayed=true, no double debit; next nonce=%d balance=%d",
		nxt.Nonce, o2.Balance("w1"))
}

// 模拟“提交中途失败”：手工把已扣款交易的持久化状态改写回 submitted，
// 再重新打开编排器，验证启动恢复把它修复为 executed，且余额/已执行表不再变化。
func TestRecoverSubmittedButLedgerApplied(t *testing.T) {
	dir := t.TempDir()
	setupWalletDir(t, dir, 1000)

	o, _ := NewOrchestrator(dir)
	registerDefaultPolicy(t, o)
	tx, _ := o.BuildTx(BuildRequest{WalletID: "w1", IdempotencyKey: "k-crash-1", To: "shop", Amount: 250})
	_, _ = o.Approve(tx.ID, "alice")
	_, _ = o.Approve(tx.ID, "carol")
	if _, err := o.Submit(tx.ID); err != nil {
		t.Fatal(err)
	}

	// 破坏状态文件：终态回退为 submitted（模拟状态推进在崩溃中丢失）。
	if err := rewriteTxStatus(filepath.Join(dir, "transactions.json"), tx.ID, StatusSubmitted); err != nil {
		t.Fatal(err)
	}

	o2, err := NewOrchestrator(dir)
	if err != nil {
		t.Fatalf("reopen during recovery: %v", err)
	}

	got, _ := o2.Get(tx.ID)
	if got.Status != StatusExecuted {
		t.Fatalf("recovered status=%s want executed", got.Status)
	}
	// 再次提交仍是重放，不重复扣款。
	r, err := o2.Submit(tx.ID)
	if err != nil || !r.Replayed {
		t.Fatalf("post-recovery submit: receipt=%+v err=%v", r, err)
	}
	if bal := o2.Balance("w1"); bal != 750 {
		t.Fatalf("balance after recovery=%d want 750", bal)
	}
	t.Logf("[crash: status lost after debit] reopen -> repaired to executed; replay replayed=true; balance=%d", balSafe(o2))
}

// 模拟“状态为 submitted 但扣款未发生”：恢复后应为 failed，Retry 后可重新提交成功。
func TestRecoverSubmittedWithoutLedgerEntry(t *testing.T) {
	dir := t.TempDir()
	setupWalletDir(t, dir, 1000)

	o, _ := NewOrchestrator(dir)
	registerDefaultPolicy(t, o)
	tx, _ := o.BuildTx(BuildRequest{WalletID: "w1", IdempotencyKey: "k-crash-2", To: "shop", Amount: 250})
	_, _ = o.Approve(tx.ID, "alice")
	_, _ = o.Approve(tx.ID, "carol")
	// 不执行 Submit，直接把状态改写为 submitted（模拟在扣款落账前崩溃）。
	if err := rewriteTxStatus(filepath.Join(dir, "transactions.json"), tx.ID, StatusSubmitted); err != nil {
		t.Fatal(err)
	}

	o2, err := NewOrchestrator(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := o2.Get(tx.ID)
	if got.Status != StatusFailed {
		t.Fatalf("recovered status=%s want failed", got.Status)
	}
	t.Logf("[crash: submitted before debit] reopen -> failed(%s); retrying", got.FailureCode)

	// failed 上直接 Submit 必须被拒绝（要求显式恢复）。
	if _, err := o2.Submit(tx.ID); ErrorCodeOf(err) != ErrCodeInvalidTransition {
		t.Fatalf("submit failed tx without retry: %v", err)
	}
	if _, err := o2.Retry(tx.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	r, err := o2.Submit(tx.ID)
	if err != nil {
		t.Fatalf("Submit after recovery retry: %v", err)
	}
	if r.Status != StatusExecuted || o2.Balance("w1") != 750 {
		t.Fatalf("unexpected receipt=%+v balance=%d", r, o2.Balance("w1"))
	}
	t.Logf("[recovered failed -> retry -> executed] nonce=%d balance=%d", r.Nonce, o2.Balance("w1"))
}

func rewriteTxStatus(path, txID string, status TxStatus) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var snap storeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	for _, tx := range snap.Transactions {
		if tx.ID == txID {
			tx.Status = status
		}
	}
	out, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

func balSafe(o *Orchestrator) int64 { return o.Balance("w1") }
