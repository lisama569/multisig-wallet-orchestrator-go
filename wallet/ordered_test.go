package wallet

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// mustProposeWithSeq 提案一笔带执行序号的交易并用 alice、bob 审批到 APPROVED。
func mustProposeWithSeq(t *testing.T, o *Orchestrator, idemKey string, amount, seq int64) *Transaction {
	t.Helper()
	tx, err := o.ProposeWithSeq(idemKey, "w1", "dest", amount, seq)
	if err != nil {
		t.Fatalf("ProposeWithSeq(%s,seq=%d): %v", idemKey, seq, err)
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

func mustSubmit(t *testing.T, o *Orchestrator, txID string) *Transaction {
	t.Helper()
	tx, err := o.Submit(txID)
	if err != nil {
		t.Fatalf("Submit(%s): %v", txID, err)
	}
	return tx
}

// execWait 以带等待的方式执行一笔有序交易（等待更小序号落账）。
func execWait(t *testing.T, o *Orchestrator, txID string) *Transaction {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := o.ExecuteWait(ctx, txID)
	if err != nil {
		t.Fatalf("ExecuteWait(%s): %v", txID, err)
	}
	return tx
}

// TestOrderedExecutionIndependentOfArrival：8 路并发、几十笔交易同时
// 提交+执行，最终落账顺序与 nonce 分配必须严格按执行序号升序，与到达顺序无关。
func TestOrderedExecutionIndependentOfArrival(t *testing.T) {
	const n = 40
	o := newTestOrchestrator(t, 100000)

	// 打乱顺序提案：序号大的先到，验证落账顺序不取决于到达顺序。
	seqs := make([]int64, 0, n)
	for i := 1; i <= n; i++ {
		seqs = append(seqs, int64(i))
	}
	rnd := rand.New(rand.NewSource(42))
	rnd.Shuffle(n, func(i, j int) { seqs[i], seqs[j] = seqs[j], seqs[i] })

	bySeq := make(map[int64]*Transaction, n)
	var totalAmount int64
	for _, seq := range seqs {
		amount := int64(seq * 3) // 金额刻意不同，避免"都一样"掩盖错位
		tx := mustProposeWithSeq(t, o, fmt.Sprintf("seq-key-%d", seq), amount, seq)
		tx = mustSubmit(t, o, tx.ID)
		bySeq[seq] = tx
		totalAmount += amount
	}
	t.Logf("[ordered-arrival] proposed+submitted %d txs in shuffled order, totalAmount=%d", n, totalAmount)

	// 8 路并发同时执行（顺序打乱）。真实批量调度器用非阻塞 Execute：
	// 拿到 ErrSeqAhead（更小序号还没走完）就稍后重试，不占住 worker 干等。
	const workers = 8
	var wg sync.WaitGroup
	var qmu sync.Mutex
	queue := append([]int64(nil), seqs...)
	rnd.Shuffle(n, func(i, j int) { queue[i], queue[j] = queue[j], queue[i] })
	start := make(chan struct{})
	aheadCount := make([]int, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start // 8 路同时起跑
			for {
				qmu.Lock()
				if len(queue) == 0 {
					qmu.Unlock()
					return
				}
				seq := queue[0]
				queue = queue[1:]
				qmu.Unlock()

				tx, err := o.Execute(bySeq[seq].ID)
				if errors.Is(err, ErrSeqAhead) {
					aheadCount[w]++
					time.Sleep(time.Millisecond) // 等更小序号落账后再试
					qmu.Lock()
					queue = append(queue, seq)
					qmu.Unlock()
					continue
				}
				if err != nil {
					t.Errorf("Execute(seq=%d): %v", seq, err)
					return
				}
				if tx.Status != StatusExecuted {
					t.Errorf("Execute(seq=%d) status=%s, want EXECUTED", seq, tx.Status)
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	t.Logf("[ordered-arrival] 8 workers, ErrSeqAhead retries per worker=%v", aheadCount)

	// 落账流水必须严格按执行序号升序，且 nonce 与序号一一对应（nonce=seq-1）。
	journal := o.ledger.Journal()
	if len(journal) != n {
		t.Fatalf("journal len=%d, want %d", len(journal), n)
	}
	for i, e := range journal {
		wantSeq := int64(i + 1)
		if e.ExecSeq != wantSeq {
			t.Fatalf("journal[%d] seq=%d, want %d (ledger order must follow exec seq, not arrival)", i, e.ExecSeq, wantSeq)
		}
		if e.Nonce != wantSeq-1 {
			t.Fatalf("journal[%d] nonce=%d, want %d", i, e.Nonce, wantSeq-1)
		}
	}
	// 查询视图与账本对得上：每笔都已落账、只尝试一次。
	for seq := int64(1); seq <= n; seq++ {
		got, err := o.GetTx(bySeq[seq].ID)
		if err != nil {
			t.Fatalf("GetTx: %v", err)
		}
		if !got.Posted() || got.Status != StatusExecuted || got.Attempts != 1 {
			t.Fatalf("seq=%d status=%s posted=%v attempts=%d, want EXECUTED/posted/attempts=1", seq, got.Status, got.Posted(), got.Attempts)
		}
		if got.Nonce != seq-1 {
			t.Fatalf("seq=%d nonce=%d, want %d", seq, got.Nonce, seq-1)
		}
	}
	if bal := o.ledger.Balance("w1"); bal != 100000-totalAmount {
		t.Fatalf("wallet balance=%d, want %d", bal, 100000-totalAmount)
	}
	if bal := o.ledger.Balance("dest"); bal != totalAmount {
		t.Fatalf("dest balance=%d, want %d", bal, totalAmount)
	}
	t.Logf("[ordered-arrival] journal order=%v; all %d posted by exec-seq, wallet=%d dest=%d",
		journalSeqs(journal[:min(5, len(journal))]), n, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
}

func journalSeqs(es []JournalEntry) []int64 {
	out := make([]int64, 0, len(es))
	for _, e := range es {
		out = append(out, e.ExecSeq)
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = errors.Is

// TestExecSeqConflictRejected：同一钱包内两笔交易报同一个执行序号时，
// 后到的那笔必须被明确拒绝（ErrSeqOccupied），不能被悄悄换号或合并；
// 先到的交易不受影响，其它序号照常。并发同序号提案也只能有一笔成功。
func TestExecSeqConflictRejected(t *testing.T) {
	o := newTestOrchestrator(t, 100000)

	first := mustProposeWithSeq(t, o, "key-seq-7-a", 100, 7)
	// 不同幂等键、相同序号：必须明确拒绝。
	_, err := o.ProposeWithSeq("key-seq-7-b", "w1", "dest", 200, 7)
	t.Logf("[seq-conflict] duplicate execSeq=7 with different idemKey; err=%v", err)
	if !errors.Is(err, ErrSeqOccupied) {
		t.Fatalf("want ErrSeqOccupied, got %v", err)
	}

	// 非法序号。
	if _, err := o.ProposeWithSeq("key-bad-seq", "w1", "dest", 10, -1); !errors.Is(err, ErrInvalidSeq) {
		t.Fatalf("seq=-1 want ErrInvalidSeq, got %v", err)
	}
	t.Logf("[seq-conflict] execSeq=-1 -> ErrInvalidSeq")

	// 被拒的序号不会产生新交易，也不会顶替先到者；先到者照常走完。
	first = mustSubmit(t, o, first.ID)
	if tx := execWait(t, o, first.ID); tx.Status != StatusExecuted || tx.ExecSeq != 7 || tx.Nonce != 0 {
		t.Fatalf("first seq=7 got status=%s nonce=%d, want EXECUTED/nonce=0", tx.Status, tx.Nonce)
	}
	// 相邻序号照常落账且 nonce 顺延。
	n8 := mustSubmit(t, o, mustProposeWithSeq(t, o, "key-seq-8", 50, 8).ID)
	if tx := execWait(t, o, n8.ID); tx.Status != StatusExecuted || tx.Nonce != 1 {
		t.Fatalf("seq=8 nonce=%d, want 1", tx.Nonce)
	}
	t.Logf("[seq-conflict] loser rejected, winner seq=7 nonce=0, seq=8 nonce=1; wallet=%d",
		o.ledger.Balance("w1"))

	// 并发同序号：只有一笔成功，其余全部拿到 ErrSeqOccupied。
	const n = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, results[i] = o.ProposeWithSeq(fmt.Sprintf("race-seq99-%d", i), "w1", "dest", 5, 99)
		}(i)
	}
	close(start)
	wg.Wait()
	winners, losers := 0, 0
	for _, e := range results {
		if e == nil {
			winners++
		} else if errors.Is(e, ErrSeqOccupied) {
			losers++
		} else {
			t.Fatalf("unexpected error: %v", e)
		}
	}
	t.Logf("[seq-conflict] %d concurrent proposals on seq=99: winners=%d losers(ErrSeqOccupied)=%d", n, winners, losers)
	if winners != 1 || losers != n-1 {
		t.Fatalf("want exactly 1 winner and %d losers, got winners=%d losers=%d", n-1, winners, losers)
	}
}

// TestRetryableFailureThenRecover：有序交易执行失败时停在 RETRYABLE
// （nonce 未消耗、无流水、无半截扣款），可查询、可补做；补做成功只扣一次钱。
// 补做仍失败时结论稳定（仍是 RETRYABLE + 可区分原因），不会被悄悄置成终态。
func TestRetryableFailureThenRecover(t *testing.T) {
	o := newTestOrchestrator(t, 100) // 初始余额 100

	tx1 := mustSubmit(t, o, mustProposeWithSeq(t, o, "key-seq-1", 80, 1).ID)
	tx2 := mustSubmit(t, o, mustProposeWithSeq(t, o, "key-seq-2", 50, 2).ID)

	if tx := execWait(t, o, tx1.ID); tx.Status != StatusExecuted {
		t.Fatalf("seq1 status=%s", tx.Status)
	}
	// seq2：余额仅剩 20，扣款 50 失败。
	_, err := o.Execute(tx2.ID)
	t.Logf("[retry] seq2 amount=50 balance=20 first attempt err=%v", err)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("want ErrInsufficientFunds, got %v", err)
	}
	got, _ := o.GetTx(tx2.ID)
	if got.Status != StatusRetryable || !got.CanRetry() || got.Posted() {
		t.Fatalf("after fail: status=%s canRetry=%v posted=%v, want RETRYABLE/canRetry/not-posted", got.Status, got.CanRetry(), got.Posted())
	}
	if got.Nonce != -1 || got.Attempts != 1 {
		t.Fatalf("retryable tx nonce=%d attempts=%d, want -1/1 (nonce not consumed)", got.Nonce, got.Attempts)
	}
	if got.FailReason != ErrInsufficientFunds.Error() {
		t.Fatalf("failReason=%q, want %q", got.FailReason, ErrInsufficientFunds.Error())
	}
	// 无半截账：流水里只有 seq1，余额没变，nonce 游标未推进。
	if j := o.ledger.Journal(); len(j) != 1 || j[0].ExecSeq != 1 {
		t.Fatalf("journal=%v, want only seq1", j)
	}
	if bal := o.ledger.Balance("w1"); bal != 20 {
		t.Fatalf("wallet balance=%d, want 20 (no partial debit)", bal)
	}
	if nn, _ := o.NextNonce("w1"); nn != 1 {
		t.Fatalf("nextNonce=%d, want 1 (failed ordered tx does not consume nonce)", nn)
	}
	t.Logf("[retry] seq2 query: status=%s posted=%v canRetry=%v reason=%q attempts=%d",
		got.Status, got.Posted(), got.CanRetry(), got.FailReason, got.Attempts)

	// 不补钱再试一次：结论稳定，仍是 RETRYABLE + 同一类原因，依然无流水。
	_, err = o.Execute(tx2.ID)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("second attempt want ErrInsufficientFunds, got %v", err)
	}
	got, _ = o.GetTx(tx2.ID)
	if got.Status != StatusRetryable || got.Attempts != 2 {
		t.Fatalf("still-failing tx status=%s attempts=%d, want RETRYABLE/2", got.Status, got.Attempts)
	}
	if len(o.ledger.Journal()) != 1 {
		t.Fatalf("journal grew on failed retry: %v", o.ledger.Journal())
	}
	t.Logf("[retry] seq2 still failing: status=%s attempts=%d reason=%q (stable conclusion)",
		got.Status, got.Attempts, got.FailReason)

	// 余额补上后补做：成功落账，且整个生命周期只扣了这一次钱。
	o.ledger.Credit("w1", 100)
	tx2, err = o.Execute(tx2.ID)
	if err != nil {
		t.Fatalf("retry after funding: %v", err)
	}
	if tx2.Status != StatusExecuted || tx2.Nonce != 1 || tx2.Attempts != 3 {
		t.Fatalf("recovered seq2 status=%s nonce=%d attempts=%d, want EXECUTED/1/3", tx2.Status, tx2.Nonce, tx2.Attempts)
	}
	j := o.ledger.Journal()
	if len(j) != 2 || j[1].ExecSeq != 2 || j[1].TxID != tx2.ID {
		t.Fatalf("journal after recover=%v, want seq1 then seq2", j)
	}
	if bal := o.ledger.Balance("w1"); bal != 70 {
		t.Fatalf("wallet balance=%d, want 70 (20+100-50, charged exactly once)", bal)
	}
	if bal := o.ledger.Balance("dest"); bal != 130 {
		t.Fatalf("dest balance=%d, want 130 (80+50, no double charge)", bal)
	}
	t.Logf("[retry] seq2 recovered after funding: nonce=%d attempts=%d journal=[%d,%d] wallet=%d dest=%d",
		tx2.Nonce, tx2.Attempts, j[0].ExecSeq, j[1].ExecSeq, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
}

// TestConcurrentRetryPostsOnce：一笔 RETRYABLE 交易在余额补足后被多个客户端
// 同时重新执行，只能有一次真正落账；所有调用方拿到一致的 EXECUTED 结果。
// 事后再次发起也不会重复生效，账本与查询视图一致。
func TestConcurrentRetryPostsOnce(t *testing.T) {
	o := newTestOrchestrator(t, 30) // 余额 30，seq1 扣 10，seq2 要 100 先失败
	tx1 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k1", 10, 1).ID)
	tx2 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k2", 100, 2).ID)
	if tx := execWait(t, o, tx1.ID); tx.Status != StatusExecuted {
		t.Fatalf("seq1: %s", tx.Status)
	}
	if _, err := o.Execute(tx2.ID); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("initial failure: %v", err)
	}
	if got, _ := o.GetTx(tx2.ID); got.Status != StatusRetryable {
		t.Fatalf("seq2 status=%s, want RETRYABLE", got.Status)
	}
	// 补足余额。
	o.ledger.Credit("w1", 1000)

	const clients = 32
	var wg sync.WaitGroup
	results := make([]*Transaction, clients)
	errs := make([]error, clients)
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = o.Execute(tx2.ID)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < clients; i++ {
		if errs[i] != nil {
			t.Fatalf("client %d got err %v", i, errs[i])
		}
		if results[i].Status != StatusExecuted || results[i].Nonce != 1 || results[i].Attempts != 2 {
			t.Fatalf("client %d got status=%s nonce=%d attempts=%d, want EXECUTED/1/2",
				i, results[i].Status, results[i].Nonce, results[i].Attempts)
		}
	}
	// 只有一条 seq2 流水，恰好扣款一次。
	j := o.ledger.Journal()
	var seq2Entries int
	for _, e := range j {
		if e.ExecSeq == 2 {
			seq2Entries++
		}
	}
	if seq2Entries != 1 {
		t.Fatalf("seq2 posted %d times, want exactly 1", seq2Entries)
	}
	if bal := o.ledger.Balance("w1"); bal != 30-10+1000-100 {
		t.Fatalf("wallet balance=%d, want %d", bal, 30-10+1000-100)
	}
	if bal := o.ledger.Balance("dest"); bal != 110 {
		t.Fatalf("dest balance=%d, want 110 (10+100, single charge)", bal)
	}
	// 完成后再被发起：依旧不重复生效。
	again, err := o.Execute(tx2.ID)
	if err != nil || again.Status != StatusExecuted || again.Attempts != 2 {
		t.Fatalf("post-completion Execute: status=%s attempts=%d err=%v", again.Status, again.Attempts, err)
	}
	if bal := o.ledger.Balance("dest"); bal != 110 {
		t.Fatalf("dest balance changed after re-execute: %d", bal)
	}
	t.Logf("[concurrent-retry] %d clients retried seq2: all EXECUTED nonce=1 attempts=2; seq2 journal entries=%d; wallet=%d dest=%d",
		clients, seq2Entries, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
}

// TestOrderedRetryAcrossRestore：服务重启/状态重载后，RETRYABLE 的有序交易
// 必须原样保留并能继续补做；已落账的不重复扣，nonce 游标与序号闸门延续不乱序。
func TestOrderedRetryAcrossRestore(t *testing.T) {
	o := newTestOrchestrator(t, 100)
	tx1 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k1", 60, 1).ID)
	tx2 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k2", 80, 2).ID)
	tx3 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k3", 10, 3).ID)

	if tx := execWait(t, o, tx1.ID); tx.Status != StatusExecuted {
		t.Fatalf("seq1: %s", tx.Status)
	}
	if _, err := o.Execute(tx2.ID); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("seq2 fail: %v", err)
	}
	// seq3 在更小序号 seq2 未走完时不能越过落账。
	if _, err := o.Execute(tx3.ID); !errors.Is(err, ErrSeqAhead) {
		t.Fatalf("seq3 should be blocked by seq2, got %v", err)
	}
	got2, _ := o.GetTx(tx2.ID)
	t.Logf("[restore] pre-snapshot: seq1 EXECUTED, seq2=%s nonce=%d attempts=%d, seq3 blocked(ErrSeqAhead)",
		got2.Status, got2.Nonce, got2.Attempts)

	snap, err := o.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	r, err := Restore(snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 恢复后状态原样：seq2 仍 RETRYABLE，余额/流水一致，seq1 不重复生效。
	r2, err := r.Execute(tx2.ID) // 余额仍不足：保持 RETRYABLE
	if !errors.Is(err, ErrInsufficientFunds) || r2.Status != StatusRetryable {
		t.Fatalf("post-restore seq2: status=%s err=%v", r2.Status, err)
	}
	if replay1, err := r.Execute(tx1.ID); err != nil || replay1.Status != StatusExecuted {
		t.Fatalf("post-restore seq1 replay: status=%s err=%v", replay1.Status, err)
	}
	if bal := r.ledger.Balance("w1"); bal != 40 {
		t.Fatalf("post-restore wallet=%d, want 40 (no double charge)", bal)
	}
	if len(r.ledger.Journal()) != 1 {
		t.Fatalf("post-restore journal len=%d, want 1", len(r.ledger.Journal()))
	}

	// 补足余额后在恢复出的实例上补做 seq2；seq3 随后按序落账，nonce 连续。
	r.ledger.Credit("w1", 1000)
	r2, err = r.Execute(tx2.ID)
	// 尝试次数：快照前失败 1 次 + 恢复后未补钱再失败 1 次 + 补做成功 1 次 = 3，
	// 但流水始终只有成功的那一条（失败不留账、不重复扣）。
	if err != nil || r2.Status != StatusExecuted || r2.Nonce != 1 || r2.Attempts != 3 {
		t.Fatalf("recover seq2: status=%s nonce=%d attempts=%d err=%v", r2.Status, r2.Nonce, r2.Attempts, err)
	}
	r3, err := r.Execute(tx3.ID)
	if err != nil || r3.Status != StatusExecuted || r3.Nonce != 2 {
		t.Fatalf("seq3 after seq2: status=%s nonce=%d err=%v", r3.Status, r3.Nonce, err)
	}
	j := r.ledger.Journal()
	if len(j) != 3 || j[0].ExecSeq != 1 || j[1].ExecSeq != 2 || j[2].ExecSeq != 3 {
		t.Fatalf("journal after restore+retry=%v, want seqs 1,2,3 in order", j)
	}
	if bal := r.ledger.Balance("w1"); bal != 40+1000-80-10 {
		t.Fatalf("final wallet=%d, want %d", bal, 40+1000-80-10)
	}
	t.Logf("[restore] after restore+funding: seq2 recovered(nonce=%d,attempts=%d), seq3 nonce=%d; journal seqs=[1,2,3]; wallet=%d",
		r2.Nonce, r2.Attempts, r3.Nonce, r.ledger.Balance("w1"))
}

// TestExecuteWaitBlockingAndContext：ExecuteWait 在更小序号未走完时阻塞等待，
// ctx 超时能及时返回（状态不变）；等更小序号落账后自动被唤醒并按序执行。
func TestExecuteWaitBlockingAndContext(t *testing.T) {
	o := newTestOrchestrator(t, 100000)
	tx1 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k1", 10, 1).ID)
	tx2 := mustSubmit(t, o, mustProposeWithSeq(t, o, "k2", 20, 2).ID)

	// 先让 seq2 在超时内等待：seq1 还没执行，应等到 ctx 到期返回。
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err := o.ExecuteWait(ctx, tx2.ID)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting seq2 want context deadline, got %v", err)
	}
	if got, _ := o.GetTx(tx2.ID); got.Status != StatusSubmitted {
		t.Fatalf("timed-out wait changed status: %s", got.Status)
	}
	t.Logf("[wait] seq2 waiting on seq1 -> ctx deadline, status unchanged=SUBMITTED")

	// 多个等待者并发等 seq2；此时先执行 seq1，它们应被唤醒且只有一次落账。
	const waiters = 8
	var wg sync.WaitGroup
	rtx := make([]*Transaction, waiters)
	rerr := make([]error, waiters)
	start := make(chan struct{})
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, c2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer c2()
			rtx[i], rerr[i] = o.ExecuteWait(c, tx2.ID)
		}(i)
	}
	close(start)
	time.Sleep(50 * time.Millisecond) // 确保等待者都已进入等待
	if tx := execWait(t, o, tx1.ID); tx.Status != StatusExecuted {
		t.Fatalf("seq1: %s", tx.Status)
	}
	wg.Wait()
	for i := 0; i < waiters; i++ {
		if rerr[i] != nil || rtx[i].Status != StatusExecuted || rtx[i].Nonce != 1 {
			t.Fatalf("waiter %d: status=%v nonce=%v err=%v", i, rtx[i].Status, rtx[i].Nonce, rerr[i])
		}
	}
	if n := len(o.ledger.Journal()); n != 2 {
		t.Fatalf("journal len=%d, want 2 (no double post by woken waiters)", n)
	}
	t.Logf("[wait] %d waiters woken after seq1 posted; seq2 posted once nonce=1", waiters)
}

// TestMixedOrderedAndLegacyConcurrency：有序与无序号老交易混在同一批 8 路并发中，
// 老用法行为不变（Submit 分配 nonce、失败消耗 nonce 为 FAILED 终态），
// 有序交易仍严格按序号落账，两类互不破坏、总账一致。
func TestMixedOrderedAndLegacyConcurrency(t *testing.T) {
	o := newTestOrchestrator(t, 1000000)
	const pairs = 30 // 每对：一笔有序 seq=i + 一笔无序号老交易

	type job struct {
		txID string
		ord  bool
	}
	var jobs []job
	for i := 1; i <= pairs; i++ {
		ord := mustSubmit(t, o, mustProposeWithSeq(t, o, fmt.Sprintf("ord-%d", i), 3, int64(i)).ID)
		leg := mustSubmit(t, o, mustProposeApprove(t, o, fmt.Sprintf("leg-%d", i), 2).ID)
		jobs = append(jobs, job{ord.ID, true}, job{leg.ID, false})
	}
	rnd := rand.New(rand.NewSource(7))
	rnd.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })

	const workers = 8
	var wg sync.WaitGroup
	queue := append([]job(nil), jobs...)
	var qmu sync.Mutex
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				qmu.Lock()
				if len(queue) == 0 {
					qmu.Unlock()
					return
				}
				j := queue[0]
				queue = queue[1:]
				qmu.Unlock()
				if j.ord {
					if _, err := o.Execute(j.txID); errors.Is(err, ErrSeqAhead) {
						time.Sleep(time.Millisecond)
						qmu.Lock()
						queue = append(queue, j)
						qmu.Unlock()
						continue
					} else if err != nil {
						t.Errorf("ordered execute: %v", err)
						return
					}
				} else {
					if _, err := o.Execute(j.txID); err != nil {
						t.Errorf("legacy execute: %v", err)
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	// 全部 EXECUTED。
	for _, j := range jobs {
		got, _ := o.GetTx(j.txID)
		if got.Status != StatusExecuted {
			t.Fatalf("tx %s (ord=%v) status=%s", j.txID, j.ord, got.Status)
		}
	}
	// 有序流水按 seq 升序；有序 nonce 与无序号 nonce 共用同一游标、各自唯一。
	j := o.ledger.Journal()
	if len(j) != 2*pairs {
		t.Fatalf("journal len=%d, want %d", len(j), 2*pairs)
	}
	nonceSeen := make(map[int64]string)
	var lastOrdSeq int64
	ordCount := 0
	for _, e := range j {
		if holder, dup := nonceSeen[e.Nonce]; dup {
			t.Fatalf("nonce %d reused by %s and %s", e.Nonce, holder, e.TxID)
		}
		nonceSeen[e.Nonce] = e.TxID
		if e.ExecSeq > 0 {
			// 有序与无序号交易共用 nonce 游标，老交易 Submit 即占号，
			// 因此有序交易的 nonce 可能被老交易穿插出间隔；只需保证：
			// 有序交易在流水里的子序列严格按执行序号升序、nonce 全局唯一。
			if e.ExecSeq <= lastOrdSeq {
				t.Fatalf("ordered journal out of sequence: %d after %d", e.ExecSeq, lastOrdSeq)
			}
			lastOrdSeq = e.ExecSeq
			ordCount++
		}
	}
	if ordCount != pairs {
		t.Fatalf("ordered journal entries=%d, want %d", ordCount, pairs)
	}
	if bal := o.ledger.Balance("dest"); bal != int64(pairs)*3+int64(pairs)*2 {
		t.Fatalf("dest balance=%d, want %d", bal, pairs*5)
	}
	t.Logf("[mixed] %d ordered + %d legacy across 8 workers: all EXECUTED, unique nonces=%d, ordered seqs monotonic 1..%d, dest=%d",
		pairs, pairs, len(nonceSeen), pairs, o.ledger.Balance("dest"))
}
