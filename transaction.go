package multisig

import "time"

// TxStatus 是交易状态机中的状态。
type TxStatus string

const (
	// StatusPending 已构建、审批进行中。
	StatusPending TxStatus = "pending"
	// StatusReady 审批数已达到阈值，等待提交。
	StatusReady TxStatus = "ready"
	// StatusSubmitted 已提交，扣款执行中（在途）。
	StatusSubmitted TxStatus = "submitted"
	// StatusExecuted 扣款成功的终态。
	StatusExecuted TxStatus = "executed"
	// StatusFailed 执行失败，可显式 Retry 后重新提交。
	StatusFailed TxStatus = "failed"
	// StatusExpired 超过有效期的终态。
	StatusExpired TxStatus = "expired"
)

// Transaction 是一笔多签交易及其全部状态。
type Transaction struct {
	ID             string
	WalletID       string
	IdempotencyKey string
	Nonce          uint64
	To             string
	Amount         int64
	CreatedAt      time.Time
	ExpiresAt      time.Time
	Status         TxStatus
	Approvals      map[string]time.Time
	SubmittedAt    *time.Time
	ExecutedAt     *time.Time
	FailureCode    ErrorCode
	FailureMessage string
	// Version 用于乐观锁 CAS，每次持久化成功后递增。
	Version int
}

// ApprovalCount 返回当前去重后的审批人数。
func (t *Transaction) ApprovalCount() int { return len(t.Approvals) }

// IsExpired 判断交易在给定时刻是否已过期（过期时刻之后即失效）。
func (t *Transaction) IsExpired(now time.Time) bool {
	return !now.Before(t.ExpiresAt)
}

// IsTerminal 报告交易是否处于不可再推进的终态。
func (t *Transaction) IsTerminal() bool {
	return t.Status == StatusExecuted || t.Status == StatusExpired
}

// Clone 返回交易的深拷贝，避免调用方在锁外修改内部状态。
func (t *Transaction) Clone() *Transaction {
	if t == nil {
		return nil
	}
	cp := *t
	cp.Approvals = make(map[string]time.Time, len(t.Approvals))
	for k, v := range t.Approvals {
		cp.Approvals[k] = v
	}
	if t.SubmittedAt != nil {
		ts := *t.SubmittedAt
		cp.SubmittedAt = &ts
	}
	if t.ExecutedAt != nil {
		ts := *t.ExecutedAt
		cp.ExecutedAt = &ts
	}
	return &cp
}

// canTransition 校验状态机是否允许 from -> to。
// 允许的跃迁：
//
//	pending    -> ready | submitted | expired
//	ready      -> submitted | expired
//	submitted  -> executed | failed
//	failed     -> ready （显式 Retry 恢复）
//
// executed 与 expired 为终态，任何离开终态的跃迁都被拒绝。
func canTransition(from, to TxStatus) bool {
	switch from {
	case StatusPending:
		return to == StatusReady || to == StatusSubmitted || to == StatusExpired
	case StatusReady:
		return to == StatusSubmitted || to == StatusExpired
	case StatusSubmitted:
		return to == StatusExecuted || to == StatusFailed
	case StatusFailed:
		return to == StatusReady
	default:
		return false
	}
}
