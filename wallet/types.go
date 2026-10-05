// Package wallet 实现本地多签钱包的交易编排与授权流程。
package wallet

import "time"

// TxStatus 表示交易在其生命周期中的状态。
type TxStatus string

const (
	StatusPending   TxStatus = "PENDING"   // 已提案，等待审批
	StatusApproved  TxStatus = "APPROVED"  // 达到审批阈值，可提交
	StatusSubmitted TxStatus = "SUBMITTED" // 已分配 nonce 并提交，可执行
	StatusRetryable TxStatus = "RETRYABLE" // 执行失败但可补做（余额补上后可重新 Execute）
	StatusExecuted  TxStatus = "EXECUTED"  // 已在账本中执行（终态）
	StatusFailed    TxStatus = "FAILED"    // 被放弃/不可恢复的失败（终态，nonce 已消耗）
	StatusRejected  TxStatus = "REJECTED"  // 被拒绝（终态）
)

// Policy 描述一个多签钱包的授权策略。
type Policy struct {
	Signers   []string      // 授权签名方集合
	Threshold int           // 生效所需的最少审批数
	TTL       time.Duration // 交易自提案起的有效期
}

// Transaction 表示一笔待编排的多签交易。
type Transaction struct {
	ID             string
	IdempotencyKey string
	WalletID       string
	To             string
	Amount         int64
	Status         TxStatus
	Approvals      []string
	Nonce          int64 // 未分配时为 -1
	Seq            int64 // 业务侧执行序号；0 表示未携带（走老行为）
	Retries        int   // 进入 RETRYABLE 后被重新执行的尝试次数
	CreatedAt      time.Time
	ExpiresAt      time.Time
	FailReason     string // 最近一次失败的可读原因
	FailKind       string // 最近一次失败的错误分类（哨兵错误字符串）
}

// Retryable 报告交易当前是否处于"还能再救一次"的状态。
func (t *Transaction) Retryable() bool {
	return t.Status == StatusRetryable
}

// Settled 报告交易是否已落账成功。
func (t *Transaction) Settled() bool {
	return t.Status == StatusExecuted
}

// Terminal 报告交易是否已到达终态。
func (t *Transaction) Terminal() bool {
	switch t.Status {
	case StatusExecuted, StatusFailed, StatusRejected:
		return true
	}
	return false
}
