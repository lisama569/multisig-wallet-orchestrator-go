package multisig

import (
	"sync"
	"testing"
)

// 三个不同签名方并发审批同一笔（阈值 2）交易：三人都应成功，交易最终 ready。
func TestConcurrentDistinctApproversAllSucceed(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)
	tx := buildReadyTx(t, o, "k-app", 100)

	signers := []string{"alice", "bob", "carol"}
	errs := make([]error, len(signers))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, s := range signers {
		wg.Add(1)
		go func(i int, s string) {
			defer wg.Done()
			<-start
			_, errs[i] = o.Approve(tx.ID, s)
		}(i, s)
	}
	close(start)
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("approver %s(%d) rejected: %v", signers[i], i, e)
		}
	}
	got, _ := o.Get(tx.ID)
	if got.ApprovalCount() != 3 || got.Status != StatusReady {
		t.Fatalf("approvals=%d status=%s, want 3/ready", got.ApprovalCount(), got.Status)
	}
	t.Logf("[3 distinct approvers concurrent] -> all accepted, approvals=%d status=%s (no CAS false-reject)",
		got.ApprovalCount(), got.Status)
}

// 同一签名方并发重复审批：恰好一次成功，其余全部 duplicate_approval，审批只计一次。
func TestConcurrentDuplicateApproverExactlyOnce(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)
	tx := buildReadyTx(t, o, "k-dup", 100)

	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = o.Approve(tx.ID, "alice")
		}(i)
	}
	close(start)
	wg.Wait()

	var success, dup int
	for _, e := range errs {
		switch {
		case e == nil:
			success++
		case ErrorCodeOf(e) == ErrCodeDuplicateApproval:
			dup++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if success != 1 || dup != n-1 {
		t.Fatalf("success=%d dup=%d, want success=1 dup=%d", success, dup, n-1)
	}
	got, _ := o.Get(tx.ID)
	if got.ApprovalCount() != 1 {
		t.Fatalf("approval count=%d, want 1", got.ApprovalCount())
	}
	t.Logf("[same approver %d concurrent approvals] -> exactly 1 success, %d duplicate_approval; count stays 1",
		n, dup)
}

// 阈值恰好等于并发数时，并发完成后应可直接提交成功。
func TestConcurrentApprovalsThenSubmit(t *testing.T) {
	o, _ := newTestOrchestrator(t, nil, 10_000)
	tx := buildReadyTx(t, o, "k-app2", 200)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range []string{"bob", "carol"} {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			<-start
			if _, err := o.Approve(tx.ID, s); err != nil {
				t.Errorf("approve %s: %v", s, err)
			}
		}(s)
	}
	close(start)
	wg.Wait()

	r, err := o.Submit(tx.ID)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if r.Status != StatusExecuted || o.Balance("w1") != 9800 {
		t.Fatalf("receipt=%+v balance=%d", r, o.Balance("w1"))
	}
	t.Logf("[2 concurrent approvals meet threshold] -> submit executed, balance=%d", o.Balance("w1"))
}
