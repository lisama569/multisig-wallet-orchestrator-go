package wallet

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// proposeApproveWithSeq 携带业务执行序号提案并审批到 APPROVED。
func proposeApproveWithSeq(t *testing.T, o *Orchestrator, idemKey string, amount int64, seq int64) *Transaction {
	t.Helper()
	tx, err := o.ProposeWithSeq(idemKey, "w1", "dest", amount, seq)
	if err != nil {
		t.Fatalf("ProposeWithSeq(%s,seq=%d): %v", idemKey, seq, err)
	}
	for _, s := range []string{"alice", "bob"} {
		if _, err := o.Approve(tx.ID, s); err != nil {
			t.Fatalf("Approve(%s,%s): %v", tx.ID, s, err)
		}
	}
	return tx
}

// 场景 1：最终落账顺序由业务执行序号决定，与请求到达顺序无关；
// 8 路并发、几十笔一起上，账本与结论稳定可复现。
func TestSeqOrderIndependentOfArrival(t *testing.T) {
	o := newTestOrchestrator(t, 1_000_000)
	const n = 40
	// 按序号 1..n 创建交易，但故意打乱"到达"顺序。
	seqs := permutation(n)
	txs := make(map[int64]*Transaction, n)
	for k, seq := range seqs {
		txs[seq] = proposeApproveWithSeq(t, o, fmt.Sprintf("seq-key-%d", seq), 100, seq)
		t.Logf("arrival#%02d propose seq=%02d tx=%s", k, seq, txs[seq].ID)
	}

	// 并发提交+执行：先乱序打一轮，让大序号先撞上顺序闸门。
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	statusMu := sync.Mutex{}
	outOfOrderHits := 0
	for _, seq := range seqs {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			<-barrier
			tx := txs[seq]
			if _, err := o.Submit(tx.ID); err != nil {
				t.Errorf("Submit(%s): %v", tx.ID, err)
				return
			}
			// 未轮到自己时反复重试，直到前序全部走完——模拟多客户端批量调度。
			for {
				_, err := o.Execute(tx.ID)
				if err == nil {
					return
				}
				if errors.Is(err, ErrSeqOutOfOrder) {
					statusMu.Lock()
					outOfOrderHits++
					statusMu.Unlock()
					time.Sleep(time.Microsecond)
					continue
				}
				t.Errorf("Execute(%s seq=%d): %v", tx.ID, seq, err)
				return
			}
		}(seq)
	}
	close(barrier)
	wg.Wait()

	// 全部成功，且账本扣款总额确定。
	list, err := o.ListTxs("w1")
	if err != nil {
		t.Fatalf("ListTxs: %v", err)
	}
	settled := 0
	var prevSeq int64
	for _, tx := range list {
		if tx.Seq == 0 {
			continue
		}
		if tx.Status != StatusExecuted || !tx.Settled() {
			t.Fatalf("seq=%d tx=%s status=%s, want EXECUTED", tx.Seq, tx.ID, tx.Status)
		}
		if tx.Seq <= prevSeq {
			t.Fatalf("ListTxs not sorted by seq at %d", tx.Seq)
		}
		prevSeq = tx.Seq
		settled++
	}
	if settled != n {
		t.Fatalf("settled=%d, want %d", settled, n)
	}
	if bal := o.ledger.Balance("dest"); bal != int64(n)*100 {
		t.Fatalf("dest balance=%d, want %d", bal, int64(n)*100)
	}
	t.Logf("input: %d txs, 8-way burst arrive order=%v; out-of-order retries=%d; all EXECUTED in seq order; dest balance=%d",
		n, seqs, outOfOrderHits, o.ledger.Balance("dest"))
}

// permutation 返回 [1..n] 的确定性伪随机排列（奇数步长取模，保证遍历全序），
// 使"到达顺序"与序号顺序明显不同且每次运行可复现。
func permutation(n int) []int64 {
	out := make([]int64, 0, n)
	step := int64(7) // 与 40 互质的步长
	cur := int64(0)
	seen := make(map[int64]bool, n)
	for len(out) < n {
		cur = (cur + step) % int64(n)
		if seen[cur] {
			cur = (cur + 1) % int64(n)
		}
		seen[cur] = true
		out = append(out, cur+1)
	}
	return out
}

// 场景 2：同一钱包内重复执行序号必须明确拒绝（ErrSeqConflict），
// 后到的交易不会被替换、合并或另派号码；并发下同样如此。
func TestSeqConflictRejected(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	first, err := o.ProposeWithSeq("k-1", "w1", "dest", 10, 5)
	if err != nil {
		t.Fatalf("first ProposeWithSeq: %v", err)
	}
	t.Logf("input: seq=5 first tx=%s", first.ID)

	// 同序号后到：明确拒绝，且占用者保持不变。
	dup, err := o.ProposeWithSeq("k-2", "w1", "dest", 20, 5)
	t.Logf("input: seq=5 again from another client; tx=%v err=%v", dup, err)
	if !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("want ErrSeqConflict, got %v", err)
	}
	if dup != nil {
		t.Fatalf("conflicting propose must not create a tx, got %+v", dup)
	}
	holder, ok := o.wallets["w1"].usedSeqs[5]
	if !ok || holder != first.ID {
		t.Fatalf("seq 5 holder=%q, want %s", holder, first.ID)
	}

	// 不同序号正常受理。
	other, err := o.ProposeWithSeq("k-3", "w1", "dest", 20, 6)
	if err != nil {
		t.Fatalf("ProposeWithSeq seq=6: %v", err)
	}

	// 并发争抢一个全新序号：只能有一个赢家，其余全部拿到 ErrSeqConflict。
	const n = 16
	var wg sync.WaitGroup
	results := make([]error, n)
	barrier := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			_, results[i] = o.ProposeWithSeq(fmt.Sprintf("race-%d", i), "w1", "dest", 1, 99)
		}(i)
	}
	close(barrier)
	wg.Wait()
	winners, conflicts := 0, 0
	for _, e := range results {
		switch {
		case e == nil:
			winners++
		case errors.Is(e, ErrSeqConflict):
			conflicts++
		default:
			t.Fatalf("unexpected err %v", e)
		}
	}
	t.Logf("input: %d concurrent proposes seq=99; winners=%d conflicts=%d", n, winners, conflicts)
	if winners != 1 || conflicts != n-1 {
		t.Fatalf("winners=%d conflicts=%d, want 1/%d", winners, conflicts, n-1)
	}

	// 非法序号。
	if _, err := o.ProposeWithSeq("bad", "w1", "dest", 1, -3); !errors.Is(err, ErrInvalidSeq) {
		t.Fatalf("seq=-3 want ErrInvalidSeq, got %v", err)
	}
	// 序号占用即使在第一笔被放弃后也不释放（防止老结论被静默改写）。
	first = proposeApproveWithSeq(t, o, "k-1", 10, 5)
	if _, err := o.Submit(first.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := o.Abandon(first.ID); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if _, err := o.ProposeWithSeq("k-4", "w1", "dest", 10, 5); !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("reuse seq of abandoned tx want ErrSeqConflict, got %v", err)
	}
	t.Logf("seq=5 stays bound to abandoned tx=%s; seq=6 tx=%s unaffected", first.ID, other.ID)
}

// 场景 2b：不带序号的老用法继续工作，且不参与新序号的排序/占用。
func TestLegacyProposeWithoutSeqStillWorks(t *testing.T) {
	o := newTestOrchestrator(t, 1000)
	legacy, err := o.Propose("legacy-key", "w1", "dest", 30)
	if err != nil {
		t.Fatalf("legacy Propose: %v", err)
	}
	if legacy.Seq != 0 {
		t.Fatalf("legacy tx Seq=%d, want 0", legacy.Seq)
	}
	// 带序号的交易与无序号老交易互不干扰。
	sequenced := proposeApproveWithSeq(t, o, "seq-key", 40, 1)
	legacy = mustProposeApprove(t, o, "legacy-key", 30)
	if _, err := o.Submit(sequenced.ID); err != nil {
		t.Fatalf("Submit sequenced: %v", err)
	}
	if _, err := o.Submit(legacy.ID); err != nil {
		t.Fatalf("Submit legacy: %v", err)
	}
	// 无序号交易不被顺序闸门拦截，可直接落账。
	if _, err := o.Execute(legacy.ID); err != nil {
		t.Fatalf("Execute legacy (no seq) should not be blocked: %v", err)
	}
	got, _ := o.GetTx(legacy.ID)
	t.Logf("legacy tx=%s seq=%d status=%s; sequenced tx=%s seq=1", got.ID, got.Seq, got.Status, sequenced.ID)
	if got.Status != StatusExecuted {
		t.Fatalf("legacy tx status=%s, want EXECUTED", got.Status)
	}
	// 幂等键重放老提案，仍返回同一笔。
	again, err := o.Propose("legacy-key", "w1", "dest", 30)
	if err != nil || again.ID != legacy.ID {
		t.Fatalf("legacy idempotent replay: id=%s err=%v", again.ID, err)
	}
}

// 场景 3：余额不足 -> RETRYABLE；补上余额后重新执行成功落账，
// 失败尝试不留半截账，补做只扣一次钱；再重试仍是同一笔成功结论。
func TestRetryAfterFundingSucceeds(t *testing.T) {
	o := newTestOrchestrator(t, 50)
	tx := proposeApproveWithSeq(t, o, "k-1", 80, 1)
	if _, err := o.Submit(tx.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	first, err := o.Execute(tx.ID)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("first Execute want ErrInsufficientFunds, got %v", err)
	}
	t.Logf("attempt1: amount=80 balance=50 -> status=%s kind=%s reason=%q retryable=%v",
		first.Status, first.FailKind, first.FailReason, first.Retryable())
	if bal := o.ledger.Balance("w1"); bal != 50 {
		t.Fatalf("balance changed on failed attempt: %d", bal)
	}
	if bal := o.ledger.Balance("dest"); bal != 0 {
		t.Fatalf("dest credited on failed attempt: %d", bal)
	}

	// 余额补上。
	o.ledger.Credit("w1", 100)
	retry, err := o.Execute(tx.ID)
	if err != nil {
		t.Fatalf("retry after funding: %v", err)
	}
	t.Logf("attempt2 after funding: status=%s retries=%d; w1=%d dest=%d",
		retry.Status, retry.Retries, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
	if retry.Status != StatusExecuted || retry.Retries != 1 {
		t.Fatalf("retry: status=%s retries=%d, want EXECUTED/1", retry.Status, retry.Retries)
	}
	if bal := o.ledger.Balance("w1"); bal != 70 {
		t.Fatalf("w1 balance=%d, want 70 (charged exactly once)", bal)
	}
	if bal := o.ledger.Balance("dest"); bal != 80 {
		t.Fatalf("dest balance=%d, want 80", bal)
	}

	// 已完成交易再次发起：原结论、不重复生效。
	again, err := o.Execute(tx.ID)
	if err != nil || again.Status != StatusExecuted {
		t.Fatalf("Execute after EXECUTED: status=%s err=%v", again.Status, err)
	}
	if bal := o.ledger.Balance("dest"); bal != 80 {
		t.Fatalf("dest balance=%d after replay, want 80 (no double charge)", bal)
	}
}

// 场景 4：同一笔尚未成功的交易被多个客户端并发重新执行，
// 只能有一次真正落账，所有客户端拿到一致结论，余额只扣一次。
func TestConcurrentRetrySettlesOnce(t *testing.T) {
	o := newTestOrchestrator(t, 20)
	tx := proposeApproveWithSeq(t, o, "k-1", 30, 1)
	if _, err := o.Submit(tx.ID); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// 先制造一轮全部失败的并发重试：钱不够，谁都不能落账。
	const clients = 16
	burst := func() ([]*Transaction, []error) {
		res := make([]*Transaction, clients)
		errs := make([]error, clients)
		var wg sync.WaitGroup
		barrier := make(chan struct{})
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-barrier
				res[i], errs[i] = o.Execute(tx.ID)
			}(i)
		}
		close(barrier)
		wg.Wait()
		return res, errs
	}
	res, errs := burst()
	failOK := 0
	for i, e := range errs {
		if errors.Is(e, ErrInsufficientFunds) && res[i].Status == StatusRetryable {
			failOK++
		}
	}
	t.Logf("burst while broke: %d/%d clients got RETRYABLE+ErrInsufficientFunds, w1=%d dest=%d",
		failOK, clients, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
	if failOK != clients {
		t.Fatalf("want all %d clients to see stable RETRYABLE, got %d", clients, failOK)
	}
	if bal := o.ledger.Balance("dest"); bal != 0 {
		t.Fatalf("dest balance=%d, want 0 (nothing settled)", bal)
	}

	// 补上钱后再一轮并发补做：恰好一次落账，其余客户端拿到 EXECUTED 同一结论。
	o.ledger.Credit("w1", 100)
	res, errs = burst()
	settledCalls := 0
	for i, e := range errs {
		if e != nil || res[i].Status != StatusExecuted {
			t.Fatalf("client %d got status=%s err=%v, want EXECUTED nil", i, res[i].Status, e)
		}
		settledCalls++
	}
	got, _ := o.GetTx(tx.ID)
	t.Logf("burst after funding: all %d clients report EXECUTED; tx retries=%d; w1=%d dest=%d",
		settledCalls, got.Retries, o.ledger.Balance("w1"), o.ledger.Balance("dest"))
	if got.Status != StatusExecuted {
		t.Fatalf("tx status=%s, want EXECUTED", got.Status)
	}
	if bal := o.ledger.Balance("w1"); bal != 90 {
		t.Fatalf("w1 balance=%d, want 90 (30 charged exactly once)", bal)
	}
	if bal := o.ledger.Balance("dest"); bal != 30 {
		t.Fatalf("dest balance=%d, want 30 (settled exactly once)", bal)
	}
}

// 场景 5：服务重启/状态重载后，走到一半的交易能接着往下做：
// RETRYABLE 交易补钱后在新实例上补做成功；序号占用、顺序闸门、nonce 游标全部延续，
// 已落账的不重复扣款。
func TestRestoreThenResumeInFlight(t *testing.T) {
	o := newTestOrchestrator(t, 100)
	tx1 := proposeApproveWithSeq(t, o, "k-1", 60, 1) // 将成功
	tx2 := proposeApproveWithSeq(t, o, "k-2", 80, 2) // 余额不足 -> RETRYABLE
	tx3 := proposeApproveWithSeq(t, o, "k-3", 10, 3) // 停在 APPROVED
	for _, x := range []*Transaction{tx1, tx2} {
		if _, err := o.Submit(x.ID); err != nil {
			t.Fatalf("Submit %s: %v", x.ID, err)
		}
	}
	if _, err := o.Execute(tx1.ID); err != nil {
		t.Fatalf("Execute tx1: %v", err)
	}
	if _, err := o.Execute(tx2.ID); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("Execute tx2 want ErrInsufficientFunds, got %v", err)
	}
	t.Logf("before snapshot: tx1=EXECUTED tx2=RETRYABLE tx3=APPROVED; w1=%d", o.ledger.Balance("w1"))

	snap, err := o.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	r, err := Restore(snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// 重载后序号占用仍在：同序号提案仍被明确拒绝。
	if _, err := r.ProposeWithSeq("k-dup", "w1", "dest", 5, 2); !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("after restore seq=2 want ErrSeqConflict, got %v", err)
	}
	// 已落账交易重放不重复扣款。
	if replay, err := r.Execute(tx1.ID); err != nil || replay.Status != StatusExecuted {
		t.Fatalf("replay tx1 after restore: status=%s err=%v", replay.Status, err)
	}
	if bal := r.ledger.Balance("dest"); bal != 60 {
		t.Fatalf("dest balance=%d, want 60 after restore replay", bal)
	}
	// 没补钱时，RETRYABLE 交易继续失败且结论稳定。
	if _, err := r.Execute(tx2.ID); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("retry tx2 on restored broke instance: %v", err)
	}
	// 补钱后在新实例上补做成功，恰好扣一次。
	r.ledger.Credit("w1", 100) // 余额 40 -> 140
	retry, err := r.Execute(tx2.ID)
	if err != nil || retry.Status != StatusExecuted {
		t.Fatalf("retry tx2 after restore+funding: status=%s err=%v", retry.Status, err)
	}
	if bal := r.ledger.Balance("w1"); bal != 60 {
		t.Fatalf("w1 balance=%d, want 60 (100-60+100-80)", bal)
	}
	if bal := r.ledger.Balance("dest"); bal != 140 {
		t.Fatalf("dest balance=%d, want 140 (60 + 80 once)", bal)
	}
	// tx3 在新实例上提交并执行：nonce 延续为 2，且 seq=3 在 tx2 终态后不再被拦。
	tx3, err = r.Submit(tx3.ID)
	if err != nil {
		t.Fatalf("Submit tx3 after restore: %v", err)
	}
	if tx3.Nonce != 2 {
		t.Fatalf("tx3 nonce=%d, want 2", tx3.Nonce)
	}
	if _, err := r.Execute(tx3.ID); err != nil {
		t.Fatalf("Execute tx3 after restore: %v", err)
	}
	t.Logf("after restore: tx2 retried to EXECUTED (retries=%d), tx3 nonce=2 EXECUTED; w1=%d dest=%d",
		retry.Retries, r.ledger.Balance("w1"), r.ledger.Balance("dest"))
	if bal := r.ledger.Balance("dest"); bal != 150 {
		t.Fatalf("dest balance=%d, want 150", bal)
	}
}

// 场景 6：并发压过一轮后，逐笔查询结果必须与账本对得上：
// 成功笔数*金额 == 收款方入账；没有 RETRYABLE 之外的半截状态，状态不倒退。
func TestQueryConsistentWithLedgerAfterConcurrentRound(t *testing.T) {
	o := newTestOrchestrator(t, 500) // 故意只够前几笔
	const n = 24
	txs := make([]*Transaction, n)
	for i := int64(0); i < n; i++ {
		txs[i] = proposeApproveWithSeq(t, o, fmt.Sprintf("k-%d", i+1), 50, i+1)
	}
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			if _, err := o.Submit(txs[i].ID); err != nil {
				t.Errorf("Submit: %v", err)
				return
			}
			// 收敛循环：顺序未到/余额不足都稍后再试，直到成功或达到轮询上限。
			for attempt := 0; attempt < 2000; attempt++ {
				_, err := o.Execute(txs[i].ID)
				if err == nil {
					return
				}
				if !(errors.Is(err, ErrSeqOutOfOrder) || errors.Is(err, ErrInsufficientFunds)) {
					t.Errorf("Execute seq=%d: unexpected err %v", txs[i].Seq, err)
					return
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}
	close(barrier)
	wg.Wait()

	// 逐笔查询并与账本核对。
	list, _ := o.ListTxs("w1")
	executed := int64(0)
	retryable := int64(0)
	for _, tx := range list {
		if tx.Seq == 0 {
			continue
		}
		switch tx.Status {
		case StatusExecuted:
			executed++
		case StatusRetryable:
			retryable++
			if !tx.Retryable() || tx.FailKind == "" {
				t.Fatalf("seq=%d RETRYABLE but Retryable()=%v kind=%q", tx.Seq, tx.Retryable(), tx.FailKind)
			}
		case StatusSubmitted:
			// 余额不足的整批交易里，未轮到尝试的交易允许停在 SUBMITTED。
		default:
			t.Fatalf("seq=%d unexpected status %s after round", tx.Seq, tx.Status)
		}
	}
	submitted := n - executed - retryable
	if submitted < 0 {
		t.Fatalf("executed=%d retryable=%d, sum exceeds %d", executed, retryable, n)
	}
	got := o.ledger.Balance("dest")
	want := executed * 50
	t.Logf("after concurrent round: executed=%d retryable=%d submitted=%d; ledger dest=%d (want %d), w1=%d",
		executed, retryable, submitted, got, want, o.ledger.Balance("w1"))
	if got != want {
		t.Fatalf("ledger dest=%d, want %d", got, want)
	}
	if bal := o.ledger.Balance("w1"); bal != 500-want {
		t.Fatalf("w1 balance=%d, want %d", o.ledger.Balance("w1"), 500-want)
	}

	// 补钱后把剩余 RETRYABLE 全部救回，再次核对：无一笔丢失、无一笔重复扣。
	o.ledger.Credit("w1", 5000)
	rescuePasses := 0
	for {
		pending := false
		for _, tx := range list {
			if tx.Status == StatusRetryable || tx.Status == StatusSubmitted {
				pending = true
				if _, err := o.Execute(tx.ID); err != nil && !errors.Is(err, ErrSeqOutOfOrder) {
					t.Fatalf("rescue seq=%d: %v", tx.Seq, err)
				}
			}
		}
		list, _ = o.ListTxs("w1")
		if !pending {
			break
		}
		rescuePasses++
		if rescuePasses > n+1 {
			t.Fatalf("rescue did not converge after %d passes", rescuePasses)
		}
	}
	list, _ = o.ListTxs("w1")
	for _, tx := range list {
		if tx.Seq != 0 && tx.Status != StatusExecuted {
			t.Fatalf("seq=%d status=%s after rescue, want EXECUTED", tx.Seq, tx.Status)
		}
	}
	if bal := o.ledger.Balance("dest"); bal != n*50 {
		t.Fatalf("final dest balance=%d, want %d", bal, n*50)
	}
	t.Logf("all %d rescued; final dest=%d w1=%d; query matches ledger", n,
		o.ledger.Balance("dest"), o.ledger.Balance("w1"))
}
