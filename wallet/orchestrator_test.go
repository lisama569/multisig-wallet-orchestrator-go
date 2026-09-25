package wallet

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestOrchestrator(t *testing.T, balance int64) *Orchestrator {
	t.Helper()
	o := NewOrchestrator(NewLedger())
	policy := Policy{Signers: []string{"alice", "bob", "carol"}, Threshold: 2, TTL: time.Hour}
	if err := o.CreateWallet("w1", policy, balance); err != nil {
		t.Fatalf("CreateWallet: %v", err)
	}
	t.Logf("wallet=w1 policy={signers:%v threshold:%d ttl:%v} balance=%d", policy.Signers, policy.Threshold, policy.TTL, balance)
	return o
}

// mustProposeApprove 提案并用 a、b 两个签名方审批到 APPROVED。
func mustProposeApprove(t *testing.T, o *Orchestrator, idemKey string, amount int64) *Transaction {
	t.Helper()
	tx, err := o.Propose(idemKey, "w1", "dest", amount)
	if err != nil {
		t.Fatalf("Propose(%s): %v", idemKey, err)
	}
	for _, s := range []string{"alice", "bob"} {
		tx, err = o.Approve(tx.ID, s)
		if err != nil {
			t.Fatalf("Approve(%s,%s): %v", tx.ID, s, err)
		}
	}
	if tx.Status != StatusApproved {
		t.Fatalf("tx %s status=%s, want APPROVED", tx.ID, tx.Status)
	}
	return tx
}

func TestProposeIdempotentReplay(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	tx1, err := o.Propose("key-1", "w1", "dest", 100)
	if err != nil {
		t.Fatalf("first Propose: %v", err)
	}
	tx2, err := o.Propose("key-1", "w1", "dest", 100)
	if err != nil {
		t.Fatalf("replay Propose: %v", err)
	}
	t.Logf("input: idemKey=key-1 twice; first id=%s replay id=%s", tx1.ID, tx2.ID)
	if tx1.ID != tx2.ID {
		t.Fatalf("idempotent replay created new tx: %s vs %s", tx1.ID, tx2.ID)
	}
}

func TestApproveAuthorizationAndDuplicate(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	tx, err := o.Propose("key-1", "w1", "dest", 100)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	_, err = o.Approve(tx.ID, "mallory")
	t.Logf("input: signer=mallory (not in policy); err=%v", err)
	if !errors.Is(err, ErrUnauthorizedSigner) {
		t.Fatalf("want ErrUnauthorizedSigner, got %v", err)
	}

	if _, err = o.Approve(tx.ID, "alice"); err != nil {
		t.Fatalf("Approve alice: %v", err)
	}
	_, err = o.Approve(tx.ID, "alice")
	t.Logf("input: signer=alice twice; err=%v", err)
	if !errors.Is(err, ErrDuplicateApproval) {
		t.Fatalf("want ErrDuplicateApproval, got %v", err)
	}

	tx, err = o.Approve(tx.ID, "bob")
	if err != nil {
		t.Fatalf("Approve bob: %v", err)
	}
	t.Logf("approvals=%v threshold=2 -> status=%s", sortedApprovals(tx), tx.Status)
	if tx.Status != StatusApproved {
		t.Fatalf("status=%s, want APPROVED", tx.Status)
	}
}

func TestExpiryRejected(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	base := time.Now()
	current := base
	o.SetClock(func() time.Time { return current })

	tx, err := o.Propose("key-1", "w1", "dest", 100)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	current = base.Add(2 * time.Hour) // 越过 TTL=1h
	_, err = o.Approve(tx.ID, "alice")
	t.Logf("input: approve at now=%s expiresAt=%s; err=%v", current, tx.ExpiresAt, err)
	if !errors.Is(err, ErrTxExpired) {
		t.Fatalf("want ErrTxExpired, got %v", err)
	}
	got, _ := o.GetTx(tx.ID)
	if got.Status != StatusRejected {
		t.Fatalf("status=%s, want REJECTED", got.Status)
	}
}

func TestConcurrentSubmitUniqueNonces(t *testing.T) {
	o := newTestOrchestrator(t, 100000)
	const n = 20
	txs := make([]*Transaction, n)
	for i := 0; i < n; i++ {
		txs[i] = mustProposeApprove(t, o, fmt.Sprintf("key-%d", i), 10)
	}

	// 并发提交 + 对同一交易并发重复提交（重复提交应幂等）。
	var wg sync.WaitGroup
	results := make([]*Transaction, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = o.Submit(txs[i].ID)
		}(i)
		go func(i int) {
			defer wg.Done()
			dup, err := o.Submit(txs[i].ID) // 重复提交：幂等
			if err != nil {
				t.Errorf("duplicate Submit(%s): %v", txs[i].ID, err)
				return
			}
			if dup.Nonce < 0 {
				t.Errorf("duplicate Submit(%s) returned nonce=%d", txs[i].ID, dup.Nonce)
			}
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]string)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("Submit(%s): %v", txs[i].ID, errs[i])
		}
		if holder, dup := seen[results[i].Nonce]; dup {
			t.Fatalf("nonce %d assigned to both %s and %s", results[i].Nonce, holder, txs[i].ID)
		}
		seen[results[i].Nonce] = txs[i].ID
	}
	t.Logf("input: %d concurrent submits; nonces assigned: %v (unique=%d)", n, keysOf(seen), len(seen))
	if len(seen) != n {
		t.Fatalf("got %d unique nonces, want %d", len(seen), n)
	}
	nn, _ := o.NextNonce("w1")
	if nn != int64(n) {
		t.Fatalf("nextNonce=%d, want %d", nn, n)
	}
}

func keysOf(m map[int64]string) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestConcurrentProposeSameKey(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	const n = 10
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := o.Propose("same-key", "w1", "dest", 50)
			if err != nil {
				t.Errorf("Propose: %v", err)
				return
			}
			ids[i] = tx.ID
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("concurrent propose with same key produced %s and %s", ids[0], ids[i])
		}
	}
	t.Logf("input: %d concurrent proposes idemKey=same-key; all got id=%s", n, ids[0])
}

func TestExecuteFailureConsumesNonceAndReplaySafe(t *testing.T) {
	o := newTestOrchestrator(t, 100) // 余额只有 100
	tx1 := mustProposeApprove(t, o, "key-1", 80)
	tx2 := mustProposeApprove(t, o, "key-2", 50)

	if _, err := o.Submit(tx1.ID); err != nil {
		t.Fatalf("Submit tx1: %v", err)
	}
	if _, err := o.Submit(tx2.ID); err != nil {
		t.Fatalf("Submit tx2: %v", err)
	}

	if _, err := o.Execute(tx1.ID); err != nil {
		t.Fatalf("Execute tx1: %v", err)
	}
	// tx2 余额不足：进入 FAILED，nonce 已消耗，不部分扣款。
	_, err := o.Execute(tx2.ID)
	t.Logf("input: execute tx2 amount=50 balance=20; err=%v", err)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("want ErrInsufficientFunds, got %v", err)
	}
	got2, _ := o.GetTx(tx2.ID)
	if got2.Status != StatusFailed {
		t.Fatalf("tx2 status=%s, want FAILED", got2.Status)
	}
	if bal := o.ledger.Balance("w1"); bal != 20 {
		t.Fatalf("wallet balance=%d, want 20 (no partial debit)", bal)
	}

	// 失败重放：再次 Execute 不得重复扣款、状态不倒退。
	replay, err := o.Execute(tx2.ID)
	t.Logf("replay Execute(%s) on FAILED: status=%s err=%v", tx2.ID, replay.Status, err)
	if err != nil || replay.Status != StatusFailed {
		t.Fatalf("replay on FAILED: status=%s err=%v", replay.Status, err)
	}
	// 成功交易重放同样幂等。
	replay1, err := o.Execute(tx1.ID)
	if err != nil || replay1.Status != StatusExecuted {
		t.Fatalf("replay on EXECUTED: status=%s err=%v", replay1.Status, err)
	}
	if bal := o.ledger.Balance("w1"); bal != 20 {
		t.Fatalf("after replays balance=%d, want 20 (no double charge)", bal)
	}

	// 失败交易的 nonce 已被消耗，后续交易使用下一个 nonce。
	tx3 := mustProposeApprove(t, o, "key-3", 10)
	tx3, err = o.Submit(tx3.ID)
	if err != nil {
		t.Fatalf("Submit tx3: %v", err)
	}
	t.Logf("tx1 nonce=0 tx2(failed) nonce=1 -> tx3 nonce=%d", tx3.Nonce)
	if tx3.Nonce != 2 {
		t.Fatalf("tx3 nonce=%d, want 2 (failed tx consumed nonce 1)", tx3.Nonce)
	}
}

func TestInvalidTransitionsRejected(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	tx, err := o.Propose("key-1", "w1", "dest", 100)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_, err = o.Execute(tx.ID) // PENDING -> EXECUTED 非法
	t.Logf("input: execute on status=%s; err=%v", StatusPending, err)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
	_, err = o.Submit(tx.ID) // PENDING -> SUBMITTED 非法（未达阈值）
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
	// 状态未被非法跃迁污染。
	got, _ := o.GetTx(tx.ID)
	if got.Status != StatusPending {
		t.Fatalf("status=%s, want PENDING after rejected transitions", got.Status)
	}
}

func TestRestoreThenReplayConsistent(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	tx1 := mustProposeApprove(t, o, "key-1", 100)
	if _, err := o.Submit(tx1.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := o.Execute(tx1.ID); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	tx2 := mustProposeApprove(t, o, "key-2", 200) // 留在 APPROVED

	snap, err := o.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	r, err := Restore(snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 重放已执行交易：不重复扣款。
	replay, err := r.Execute(tx1.ID)
	t.Logf("replay Execute(%s) after restore: status=%s err=%v", tx1.ID, replay.Status, err)
	if err != nil || replay.Status != StatusExecuted {
		t.Fatalf("replay after restore: status=%s err=%v", replay.Status, err)
	}
	if bal := r.ledger.Balance("w1"); bal != 900 {
		t.Fatalf("balance=%d, want 900 (no double charge after restore)", bal)
	}
	// 重放幂等提案：返回同一交易。
	dup, err := r.Propose("key-1", "w1", "dest", 100)
	if err != nil || dup.ID != tx1.ID {
		t.Fatalf("replayed Propose: id=%s err=%v", dup.ID, err)
	}
	// 恢复后 nonce 游标延续，不回退。
	tx2, err = r.Submit(tx2.ID)
	if err != nil {
		t.Fatalf("Submit tx2 after restore: %v", err)
	}
	t.Logf("after restore: tx2 nonce=%d (tx1 consumed 0)", tx2.Nonce)
	if tx2.Nonce != 1 {
		t.Fatalf("tx2 nonce=%d, want 1", tx2.Nonce)
	}
}

func TestConcurrentMixedLifecycleDeterministic(t *testing.T) {
	o := newTestOrchestrator(t, 100000)
	const n = 8
	txs := make([]*Transaction, n)
	for i := 0; i < n; i++ {
		txs[i] = mustProposeApprove(t, o, fmt.Sprintf("key-%d", i), 10)
	}
	// 并发完成 提交+执行 全流程。
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := o.Submit(txs[i].ID); err != nil {
				t.Errorf("Submit(%s): %v", txs[i].ID, err)
				return
			}
			if _, err := o.Execute(txs[i].ID); err != nil {
				t.Errorf("Execute(%s): %v", txs[i].ID, err)
			}
		}(i)
	}
	wg.Wait()
	// 无论并发顺序如何，终态确定：全部 EXECUTED，总账一致。
	total := int64(0)
	for i := 0; i < n; i++ {
		got, _ := o.GetTx(txs[i].ID)
		if got.Status != StatusExecuted {
			t.Fatalf("tx %s status=%s, want EXECUTED", txs[i].ID, got.Status)
		}
		total += got.Amount
	}
	if bal := o.ledger.Balance("dest"); bal != total {
		t.Fatalf("dest balance=%d, want %d", bal, total)
	}
	t.Logf("input: %d concurrent submit+execute; all EXECUTED, dest balance=%d", n, total)
}
