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
	o.wallets[id] = &walletState{policy: policy, usedNonces: make(map[int64]string)}
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

// Propose 提案交易。相同幂等键重复提案返回首次创建的交易，不产生新交易。
func (o *Orchestrator) Propose(idemKey, walletID, to string, amount int64) (*Transaction, error) {
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
		CreatedAt:      now,
		ExpiresAt:      now.Add(w.policy.TTL),
	}
	o.txs[tx.ID] = tx
	o.byIdem[idemKey] = tx.ID
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
	if tx.Status == StatusSubmitted || tx.Status == StatusExecuted {
		return clone(tx), nil // 幂等重放
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
// 余额不足时交易进入 FAILED 终态且 nonce 已消耗，不会部分扣款。
// 对终态（EXECUTED/FAILED）重放是幂等 no-op，不会重复扣款。
func (o *Orchestrator) Execute(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, _, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	switch tx.Status {
	case StatusExecuted, StatusFailed:
		return clone(tx), nil // 幂等重放：不重复扣款、状态不倒退
	case StatusSubmitted:
		// 继续执行
	default:
		return clone(tx), fmt.Errorf("execute on %s: %w", tx.Status, ErrInvalidTransition)
	}
	if err := o.ledger.Debit(tx.WalletID, tx.Amount); err != nil {
		tx.Status = StatusFailed
		tx.FailReason = err.Error()
		return clone(tx), err
	}
	o.ledger.Credit(tx.To, tx.Amount)
	tx.Status = StatusExecuted
	return clone(tx), nil
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
