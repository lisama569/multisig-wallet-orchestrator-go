package wallet

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Orchestrator 编排多签交易：提案、审批、提交（nonce 分配）与执行。
// 所有状态变更都在同一把互斥锁内完成，因此并发请求被串行化，
// 结论只取决于请求被受理的顺序，不存在静默取舍。
type Orchestrator struct {
	mu      sync.Mutex
	ledger  *Ledger
	now     func() time.Time // 可注入时钟，便于测试有效期
	wallets map[string]*walletState
	txs     map[string]*Transaction
	byIdem  map[string]string // 幂等键 -> 交易 ID
	txSeq   int64
}

type walletState struct {
	policy     Policy
	nextNonce  int64
	usedNonces map[int64]string // nonce -> txID，防止在途交易互相覆盖
	usedSeqs   map[int64]string // 业务执行序号 -> txID，序号一经占用永不释放
}

// NewOrchestrator 创建编排器。
func NewOrchestrator(ledger *Ledger) *Orchestrator {
	return &Orchestrator{
		ledger:  ledger,
		now:     time.Now,
		wallets: make(map[string]*walletState),
		txs:     make(map[string]*Transaction),
		byIdem:  make(map[string]string),
	}
}

// SetClock 注入时钟（测试用）。
func (o *Orchestrator) SetClock(now func() time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.now = now
}

// CreateWallet 注册钱包及其策略与初始余额。
func (o *Orchestrator) CreateWallet(id string, policy Policy, balance int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := validatePolicy(policy); err != nil {
		return err
	}
	if _, ok := o.wallets[id]; ok {
		return fmt.Errorf("wallet %q already exists: %w", id, ErrInvalidPolicy)
	}
	o.wallets[id] = &walletState{
		policy:     policy,
		usedNonces: make(map[int64]string),
		usedSeqs:   make(map[int64]string),
	}
	o.ledger.Credit(id, balance)
	return nil
}

func validatePolicy(p Policy) error {
	if p.Threshold < 1 || p.Threshold > len(p.Signers) {
		return fmt.Errorf("threshold %d out of range [1,%d]: %w", p.Threshold, len(p.Signers), ErrInvalidPolicy)
	}
	if p.TTL <= 0 {
		return fmt.Errorf("ttl must be positive: %w", ErrInvalidPolicy)
	}
	seen := make(map[string]bool, len(p.Signers))
	for _, s := range p.Signers {
		if s == "" || seen[s] {
			return fmt.Errorf("empty or duplicate signer %q: %w", s, ErrInvalidPolicy)
		}
		seen[s] = true
	}
	return nil
}

// Propose 提案交易（不带业务执行序号，走老行为）。
// 相同幂等键重复提案返回首次创建的交易，不产生新交易。
func (o *Orchestrator) Propose(idemKey, walletID, to string, amount int64) (*Transaction, error) {
	return o.ProposeWithSeq(idemKey, walletID, to, amount, 0)
}

// ProposeWithSeq 提案交易并携带业务侧约定的执行序号 seq。
// 同一钱包内 seq 唯一：序号已被占用时，后到的这笔被明确拒绝（ErrSeqConflict），
// 不会被替换、合并或另派号码。seq 一经占用即绑定该交易，即使其后来被拒或放弃也不释放。
// seq <= 0 视为非法序号（ErrInvalidSeq）；seq == 0 的老调用方请改用 Propose。
func (o *Orchestrator) ProposeWithSeq(idemKey, walletID, to string, amount int64, seq int64) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if txID, ok := o.byIdem[idemKey]; ok {
		return clone(o.txs[txID]), nil // 幂等重放：返回既有交易
	}
	w, ok := o.wallets[walletID]
	if !ok {
		return nil, ErrWalletNotFound
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if seq < 0 {
		return nil, fmt.Errorf("seq %d must be positive (or 0 for legacy): %w", seq, ErrInvalidSeq)
	}
	if seq > 0 {
		if holder, taken := w.usedSeqs[seq]; taken {
			return nil, fmt.Errorf("wallet %s seq %d already held by tx %s: %w", walletID, seq, holder, ErrSeqConflict)
		}
	}
	o.txSeq++
	now := o.now()
	tx := &Transaction{
		ID:             fmt.Sprintf("tx-%d", o.txSeq),
		IdempotencyKey: idemKey,
		WalletID:       walletID,
		To:             to,
		Amount:         amount,
		Status:         StatusPending,
		Nonce:          -1,
		Seq:            seq,
		CreatedAt:      now,
		ExpiresAt:      now.Add(w.policy.TTL),
	}
	o.txs[tx.ID] = tx
	o.byIdem[idemKey] = tx.ID
	if seq > 0 {
		w.usedSeqs[seq] = tx.ID
	}
	return clone(tx), nil
}

// Approve 审批交易。越权、重复审批、过期均返回可区分错误。
func (o *Orchestrator) Approve(txID, signer string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, w, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	if err := o.checkExpiry(tx); err != nil {
		return clone(tx), err
	}
	if tx.Status != StatusPending && tx.Status != StatusApproved {
		return clone(tx), fmt.Errorf("approve on %s: %w", tx.Status, ErrInvalidTransition)
	}
	if !contains(w.policy.Signers, signer) {
		return clone(tx), ErrUnauthorizedSigner
	}
	if contains(tx.Approvals, signer) {
		return clone(tx), ErrDuplicateApproval
	}
	tx.Approvals = append(tx.Approvals, signer)
	if len(tx.Approvals) >= w.policy.Threshold {
		tx.Status = StatusApproved
	}
	return clone(tx), nil
}

// Submit 提交交易并为其分配钱包的下一个 nonce。
// 重复提交同一交易是幂等 no-op，返回当前状态。
func (o *Orchestrator) Submit(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, w, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	if tx.Status == StatusSubmitted || tx.Status == StatusRetryable || tx.Status == StatusExecuted {
		return clone(tx), nil // 幂等重放：已提交（含失败待补做）的交易不重复分配 nonce
	}
	if err := o.checkExpiry(tx); err != nil {
		return clone(tx), err
	}
	if tx.Status != StatusApproved {
		return clone(tx), fmt.Errorf("submit on %s: %w", tx.Status, ErrInvalidTransition)
	}
	nonce := w.nextNonce
	if holder, taken := w.usedNonces[nonce]; taken && holder != tx.ID {
		return clone(tx), fmt.Errorf("nonce %d held by %s: %w", nonce, holder, ErrNonceConflict)
	}
	w.usedNonces[nonce] = tx.ID
	w.nextNonce++
	tx.Nonce = nonce
	tx.Status = StatusSubmitted
	return clone(tx), nil
}

// Execute 执行已提交的交易并写账本。
//
// 失败与恢复语义：
//   - 余额不足等可恢复失败时，交易停在 RETRYABLE（而非终态），不留下半截账：
//     出账要么整笔成功，要么余额原样保留；待余额补上后再次 Execute 即可补做并落账。
//   - RETRYABLE 上的每次失败尝试只改状态标记，不扣钱；成功时恰好扣一次。
//   - EXECUTED 重放是幂等 no-op：不重复扣款、状态不倒退，并发重放拿到同一结论。
//   - FAILED/REJECTED 为不可补做的终态，对其再 Execute 返回 ErrNotRetryable。
//
// 顺序语义：携带执行序号的交易，只有当同钱包内更小序号的在途交易都已走完
// （到达终态）时才允许落账；否则返回 ErrSeqOutOfOrder，交易保持 SUBMITTED，稍后重试即可。
func (o *Orchestrator) Execute(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, w, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	switch tx.Status {
	case StatusExecuted:
		return clone(tx), nil // 幂等重放：不重复扣款、状态不倒退
	case StatusSubmitted, StatusRetryable:
		// 可执行 / 可补做
	case StatusFailed, StatusRejected:
		return clone(tx), fmt.Errorf("execute on terminal %s: %w", tx.Status, ErrNotRetryable)
	default:
		return clone(tx), fmt.Errorf("execute on %s: %w", tx.Status, ErrInvalidTransition)
	}
	if tx.Status == StatusRetryable {
		tx.Retries++
	}
	// 顺序闸门：更小序号的在途交易尚未走完时，本笔不得落账。
	if holder, ok := o.seqBlocker(w, tx); ok {
		if tx.Status == StatusSubmitted {
			tx.Retries++ // 记为一次未成功的执行尝试
		}
		tx.FailReason = ErrSeqOutOfOrder.Error()
		tx.FailKind = kindOf(ErrSeqOutOfOrder)
		return clone(tx), fmt.Errorf("seq %d waits on tx %s: %w", tx.Seq, holder, ErrSeqOutOfOrder)
	}
	if err := o.ledger.Debit(tx.WalletID, tx.Amount); err != nil {
		// 可恢复失败：停在 RETRYABLE，钱没动，等条件满足后补做。
		tx.Status = StatusRetryable
		tx.FailReason = err.Error()
		tx.FailKind = kindOf(ErrInsufficientFunds)
		return clone(tx), err
	}
	o.ledger.Credit(tx.To, tx.Amount)
	tx.Status = StatusExecuted
	tx.FailReason = ""
	tx.FailKind = ""
	return clone(tx), nil
}

// Abandon 放弃一笔可补做（RETRYABLE）或尚未执行（SUBMITTED）的交易，
// 使其进入 FAILED 终态：不再可重试，nonce 已消耗，绝不扣款。
// 对终态交易重放是幂等 no-op。
func (o *Orchestrator) Abandon(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, _, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	switch tx.Status {
	case StatusFailed:
		return clone(tx), nil
	case StatusRetryable, StatusSubmitted:
		tx.Status = StatusFailed
		if tx.FailReason == "" {
			tx.FailReason = "abandoned before execution"
		}
		return clone(tx), nil
	default:
		return clone(tx), fmt.Errorf("abandon on %s: %w", tx.Status, ErrInvalidTransition)
	}
}

// seqBlocker 返回阻塞 tx 落账的更小序号在途交易 ID。
// 只有携带 seq 的交易参与排序；更小序号交易一旦到达终态即不再阻塞。
func (o *Orchestrator) seqBlocker(w *walletState, tx *Transaction) (string, bool) {
	if tx.Seq == 0 {
		return "", false
	}
	for s, otherID := range w.usedSeqs {
		if s >= tx.Seq || otherID == tx.ID {
			continue
		}
		other, ok := o.txs[otherID]
		if !ok || other.Terminal() {
			continue
		}
		// 前序交易若已过期，借此次闸门检查把它送入 REJECTED 终态，避免永久堵队。
		if (other.Status == StatusPending || other.Status == StatusApproved) &&
			!o.now().Before(other.ExpiresAt) {
			other.Status = StatusRejected
			other.FailReason = ErrTxExpired.Error()
			other.FailKind = kindOf(ErrTxExpired)
			continue
		}
		return otherID, true
	}
	return "", false
}

// kindOf 返回哨兵错误的稳定分类标识（写入 Transaction.FailKind，供查询区分）。
func kindOf(target error) string {
	return target.Error()
}

// GetTx 查询交易当前状态。
func (o *Orchestrator) GetTx(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, ok := o.txs[txID]
	if !ok {
		return nil, ErrTxNotFound
	}
	return clone(tx), nil
}

// ListTxs 返回某钱包的全部交易，按执行序号（seq 相同/缺失时按内部交易号）升序排列，
// 便于批量调度压测后逐笔与账本核对。
func (o *Orchestrator) ListTxs(walletID string) ([]*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.wallets[walletID]; !ok {
		return nil, ErrWalletNotFound
	}
	var out []*Transaction
	for _, tx := range o.txs {
		if tx.WalletID == walletID {
			out = append(out, clone(tx))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// NextNonce 返回钱包下一个可用 nonce（只读）。
func (o *Orchestrator) NextNonce(walletID string) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	w, ok := o.wallets[walletID]
	if !ok {
		return 0, ErrWalletNotFound
	}
	return w.nextNonce, nil
}

// loadTx 取出交易及其钱包状态。
func (o *Orchestrator) loadTx(txID string) (*Transaction, *walletState, error) {
	tx, ok := o.txs[txID]
	if !ok {
		return nil, nil, ErrTxNotFound
	}
	w := o.wallets[tx.WalletID]
	return tx, w, nil
}

// checkExpiry 将过期的在途交易置为 REJECTED 终态并返回 ErrTxExpired。
func (o *Orchestrator) checkExpiry(tx *Transaction) error {
	if tx.Status != StatusPending && tx.Status != StatusApproved {
		return nil
	}
	if !o.now().Before(tx.ExpiresAt) {
		tx.Status = StatusRejected
		tx.FailReason = ErrTxExpired.Error()
		return ErrTxExpired
	}
	return nil
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// clone 返回交易副本，避免调用方修改内部状态。
func clone(tx *Transaction) *Transaction {
	c := *tx
	c.Approvals = append([]string(nil), tx.Approvals...)
	return &c
}

// Approvals 的确定性展示（测试日志用）。
func sortedApprovals(tx *Transaction) []string {
	out := append([]string(nil), tx.Approvals...)
	sort.Strings(out)
	return out
}
