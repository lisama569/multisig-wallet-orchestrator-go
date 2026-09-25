package multisig_test

import (
	"fmt"
	"os"
	"time"

	multisig "github.com/lisama569/multisig-wallet-orchestrator-go"
)

func Example() {
	dir, err := os.MkdirTemp("", "multisig-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	o, err := multisig.NewOrchestrator(dir)
	if err != nil {
		panic(err)
	}
	policy, err := multisig.NewPolicy("treasury", []string{"alice", "bob", "carol"}, 2, 24*time.Hour)
	if err != nil {
		panic(err)
	}
	if err := o.AddPolicy(policy); err != nil {
		panic(err)
	}
	if err := o.Deposit("treasury", 1000); err != nil {
		panic(err)
	}

	tx, err := o.BuildTx(multisig.BuildRequest{
		WalletID: "treasury", IdempotencyKey: "payment-2026-09-25-001",
		To: "supplier", Amount: 300,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("built nonce=%d status=%s\n", tx.Nonce, tx.Status)

	// 越权审批被拒绝，原因可区分。
	if _, err := o.Approve(tx.ID, "mallory"); err != nil {
		fmt.Println("reject:", multisig.ErrorCodeOf(err))
	}
	// 两名合法签名方审批达到阈值。
	if _, err := o.Approve(tx.ID, "alice"); err != nil {
		panic(err)
	}
	if _, err := o.Approve(tx.ID, "bob"); err != nil {
		panic(err)
	}

	receipt, err := o.Submit(tx.ID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("submitted nonce=%d status=%s replayed=%v balance=%d\n",
		receipt.Nonce, receipt.Status, receipt.Replayed, o.Balance("treasury"))

	// 重复提交命中幂等：不重复扣款。
	again, _ := o.Submit(tx.ID)
	fmt.Printf("replay status=%s replayed=%v balance=%d\n",
		again.Status, again.Replayed, o.Balance("treasury"))

	// Output:
	// built nonce=0 status=pending
	// reject: unauthorized_approver
	// submitted nonce=0 status=executed replayed=false balance=700
	// replay status=executed replayed=true balance=700
}
