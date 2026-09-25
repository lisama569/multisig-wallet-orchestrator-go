package multisig

import (
	"fmt"
	"sync"
	"testing"
)

// approveAndReady 为交易补齐阈值审批。
func approveAndReady(t *testing.T, o *Orchestrator, tx *Transaction, signers ...string) {
	t.Helper()
	for _, s := range signers {
		if _, err := o.Approve(tx.ID, s); err != nil {
			t.Fatalf("Approve %s: %v", s, err)
		}
	}
}

func TestNonceAssignmentAndOutOfOrderReject(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)

	tx0 := buildReadyTx(t, o, "k0", 100)
	tx1 := buildReadyTx(t, o, "k1", 100)
	tx2 := buildReadyTx(t, o, "k2", 100)
	t.Logf("[built] nonces assigned: %s=%d %s=%d %s=%d",
		tx0.ID, tx0.Nonce, tx1.ID, tx1.Nonce, tx2.ID, tx2.Nonce)
	if tx0.Nonce != 0 || tx1.Nonce != 1 || tx2.Nonce != 2 {
		t.Fatalf("nonce assignment must be sequential: %d,%d,%d", tx0.Nonce, tx1.Nonce, tx2.Nonce)
	}
	approveAndReady(t, o, tx0, "alice", "bob")
	approveAndReady(t, o, tx1, "alice", "bob")
	approveAndReady(t, o, tx2, "alice", "bob")

	// 先提交 nonce=2：必须被明确拒绝（在途的 nonce=0,1 阻塞），而不是静默排队执行。
	if _, err := o.Submit(tx2.ID); ErrorCodeOf(err) != ErrCodeNonceNotNext {
		t.Fatalf("out-of-order submit must be rejected: %v", err)
	}
	t.Logf("[submit tx nonce=2 first] -> reject code=%s reason=%q",
		ErrCodeNonceNotNext, errMsg(o, tx2.ID))

	// 按序提交 0、1，再提交 2：全部成功，账本 nonce 连续。
	for _, tx := range []*Transaction{tx0, tx1, tx2} {
		r, err := o.Submit(tx.ID)
		if err != nil {
			t.Fatalf("Submit nonce=%d: %v", tx.Nonce, err)
		}
		if !r.Replayed && r.Status != StatusExecuted {
			t.Fatalf("unexpected receipt: %+v", r)
		}
	}
	if last, _ := o.ledger.LastNonce("w1"); last != 2 {
		t.Fatalf("ledger last nonce = %d, want 2", last)
	}
	if bal := o.Balance("w1"); bal != 9700 {
		t.Fatalf("balance = %d, want 9700", bal)
	}
	t.Logf("[in-order submit 0,1,2] -> all executed; ledger lastNonce=2 balance=9700")
}

func errMsg(o *Orchestrator, txID string) string {
	_, err := o.Submit(txID)
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestIdempotentBuildUnderConcurrency(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)

	const n = 32
	var wg sync.WaitGroup
	results := make([]*Transaction, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = o.BuildTx(BuildRequest{
				WalletID: "w1", IdempotencyKey: "same-key", To: "shop", Amount: 100,
			})
		}(i)
	}
	close(start)
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("concurrent BuildTx[%d] error: %v", i, e)
		}
	}
	id := results[0].ID
	nonce := results[0].Nonce
	for i, r := range results {
		if r.ID != id || r.Nonce != nonce {
			t.Fatalf("concurrent builds diverged at %d: got id=%s nonce=%d, want id=%s nonce=%d",
				i, r.ID, r.Nonce, id, nonce)
		}
	}
	t.Logf("[%d concurrent builds with same idempotency key] -> single tx id=%s nonce=%d, no duplicate nonce",
		n, id, nonce)

	// 下一笔不同 key 的交易必须拿到下一个 nonce，不能因为并发预留而空洞。
	next := buildReadyTx(t, o, "other-key", 10)
	if next.Nonce != nonce+1 {
		t.Fatalf("next nonce = %d, want %d (no nonce hole after concurrent dedup)", next.Nonce, nonce+1)
	}
}

func TestConcurrentSubmitSameTxConverges(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)
	tx := buildReadyTx(t, o, "k-conv", 123)
	approveAndReady(t, o, tx, "alice", "carol")

	const n = 24
	var wg sync.WaitGroup
	receipts := make([]*Receipt, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			receipts[i], errs[i] = o.Submit(tx.ID)
		}(i)
	}
	close(start)
	wg.Wait()

	var executed, replayed int
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d unexpected error: %v", i, errs[i])
		}
		if receipts[i].Status != StatusExecuted {
			t.Fatalf("submit %d status=%s", i, receipts[i].Status)
		}
		if receipts[i].Replayed {
			replayed++
		} else {
			executed++
		}
	}
	if executed != 1 {
		t.Fatalf("exactly one fresh execution expected, got %d (replayed=%d)", executed, replayed)
	}
	if bal := o.Balance("w1"); bal != 10_000-123 {
		t.Fatalf("balance=%d want=%d (must debit exactly once)", bal, 10_000-123)
	}
	t.Logf("[%d concurrent submits same tx] -> exactly 1 fresh execute + %d replays; single debit, balance=%d",
		n, replayed, 10_000-123)
}

func TestConcurrentDistinctTxsNoDuplicateNonceEffect(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 1_000_000)
	const n = 20
	// 先构建并审批 n 笔，nonce 已串行分配；再并发乱序提交。
	txs := make([]*Transaction, n)
	for i := 0; i < n; i++ {
		txs[i] = buildReadyTx(t, o, fmt.Sprintf("key-%d", i), 10)
		approveAndReady(t, o, txs[i], "alice", "bob")
	}

	var wg sync.WaitGroup
	okRej := make([]string, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if _, err := o.Submit(txs[i].ID); err != nil {
				if ErrorCodeOf(err) != ErrCodeNonceNotNext {
					okRej[i] = "unexpected:" + err.Error()
					return
				}
				okRej[i] = "rejected-not-next"
			} else {
				okRej[i] = "executed"
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// 无论到达顺序如何，最终驱动所有交易按 nonce 顺序完成。
	for {
		progressed := false
		for i := 0; i < n; i++ {
			if got, _ := o.Get(txs[i].ID); got.Status != StatusExecuted {
				if _, err := o.Submit(txs[i].ID); err == nil {
					progressed = true
				}
			}
		}
		if !progressed {
			break
		}
	}
	for i := 0; i < n; i++ {
		got, _ := o.Get(txs[i].ID)
		if got.Status != StatusExecuted {
			t.Fatalf("tx nonce=%d not executed: %s", txs[i].Nonce, got.Status)
		}
	}
	if last, _ := o.ledger.LastNonce("w1"); last != n-1 {
		t.Fatalf("ledger lastNonce=%d want %d", last, n-1)
	}
	if bal := o.Balance("w1"); bal != 1_000_000-10*n {
		t.Fatalf("balance=%d want=%d", bal, 1_000_000-10*n)
	}
	t.Logf("[%d distinct txs concurrent out-of-order] -> rejections are nonce_not_next; final in-order execution debits exactly %d, balance=%d",
		n, 10*n, 1_000_000-10*n)
}
