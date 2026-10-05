// Package wallet 实现本地多签钱包的交易编排与授权流程。
package wallet

import "time"

// TxStatus 表示交易在其生命周期中的状态。
type TxStatus string

const (
	StatusPending   TxStatus = "PENDING"   // 已提案，等待审批
	StatusApproved  TxStatus = "APPROVED"  // 达到审批阈值，可提交
	StatusSubmitted TxStatus = "SUBMITTED" // 已提交：有序交易等待按执行序号落账
	StatusExecuted  TxStatus = "EXECUTED"  // 已在账本中执行（终态）
	StatusFailed    TxStatus = "FAILED"    // 执行失败（终态，nonce 已消耗；仅无序号老交易）
	StatusRetryable TxStatus = "RETRYABLE" // 执行失败但可补做（有序交易，nonce 未消耗，无半截账）
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
	CreatedAt      time.Time
	ExpiresAt      time.Time
	FailReason     string
	// ExecSeq 是业务侧约定的执行序号（同一钱包内唯一）。
	// 0 表示不带序号的老用法：不参与有序落账，失败即终态并消耗 nonce。
	// 大于 0 时交易按 ExecSeq 升序落账，失败可补做且不消耗 nonce。
	ExecSeq int64
	// Attempts 是实际执行（扣款尝试）次数；每次真正调用账本执行时加一。
	Attempts int
}

// Ordered 报告交易是否携带业务执行序号（有序交易）。
func (t *Transaction) Ordered() bool { return t.ExecSeq > 0 }

// Posted 报告交易是否已真正落账（扣款成功）。
func (t *Transaction) Posted() bool { return t.Status == StatusExecuted }

// CanRetry 报告交易当前是否处于"还能再救一次"的可补做状态。
func (t *Transaction) CanRetry() bool { return t.Status == StatusRetryable }
