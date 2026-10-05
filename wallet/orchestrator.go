package wallet

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Orchestrator 编排多签交易：提案、审批、提交（nonce 分配）与执行。
// 所有状态变更都在同一把互斥锁内完成，因此并发请求被串行化；
// 携带业务执行序号的交易，其落账顺序与结论只取决于执行序号，
// 与请求到达顺序无关。无序号老交易的行为保持兼容。
type Orchestrator struct {
	mu      sync.Mutex
	cond    *sync.Cond // 与 mu 绑定：有序交易等待更小序号走完时阻塞/唤醒
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
	seqs       map[int64]string // 执行序号 -> txID，保证同钱包序号唯一
}

// NewOrchestrator 创建编排器。
func NewOrchestrator(ledger *Ledger) *Orchestrator {
	o := &Orchestrator{
		ledger:  ledger,
		now:     time.Now,
		wallets: make(map[string]*walletState),
		txs:     make(map[string]*Transaction),
		byIdem:  make(map[string]string),
	}
	o.cond = sync.NewCond(&o.mu)
	return o
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
	o.wallets[id] = &walletState{policy: policy, usedNonces: make(map[int64]string), seqs: make(map[int64]string)}
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

// Propose 以不带执行序号的老用法提案交易（等价于 ProposeWithSeq(..., 0)）。
// 相同幂等键重复提案返回首次创建的交易，不产生新交易。
func (o *Orchestrator) Propose(idemKey, walletID, to string, amount int64) (*Transaction, error) {
	return o.propose(idemKey, walletID, to, amount, 0)
}

// ProposeWithSeq 提案一笔携带业务执行序号的交易。
// 同一钱包内 execSeq 必须唯一：若序号已被先到的交易占用（哪怕对方是并发到达），
// 后到的这笔被明确拒绝并返回 ErrSeqOccupied，不会被替换、合并或换号。
// execSeq 仅要求 > 0，不要求连续；execSeq<=0 返回 ErrInvalidSeq。
// 相同幂等键的重放仍返回首次创建的交易（幂等优先）。
func (o *Orchestrator) ProposeWithSeq(idemKey, walletID, to string, amount, execSeq int64) (*Transaction, error) {
	return o.propose(idemKey, walletID, to, amount, execSeq)
}

func (o *Orchestrator) propose(idemKey, walletID, to string, amount, execSeq int64) (*Transaction, error) {
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
	if execSeq < 0 {
		return nil, fmt.Errorf("execSeq %d must be positive or 0: %w", execSeq, ErrInvalidSeq)
	}
	if execSeq > 0 {
		if holder, taken := w.seqs[execSeq]; taken {
			return nil, fmt.Errorf("wallet %q execSeq %d already held by tx %s: %w", walletID, execSeq, holder, ErrSeqOccupied)
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
		CreatedAt:      now,
		ExpiresAt:      now.Add(w.policy.TTL),
		ExecSeq:        execSeq,
	}
	o.txs[tx.ID] = tx
	o.byIdem[idemKey] = tx.ID
	if execSeq > 0 {
		w.seqs[execSeq] = tx.ID
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

// Submit 提交交易。
//   - 无序号老交易：在 Submit 时分配钱包下一个 nonce（失败即终态并消耗 nonce）；
//   - 带执行序号的交易：Submit 不分配 nonce，nonce 在按序号成功落账时才分配，
//     失败可补做且不消耗 nonce。
//
// 重复提交同一交易是幂等 no-op，返回当前状态。
func (o *Orchestrator) Submit(txID string) (*Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tx, w, err := o.loadTx(txID)
	if err != nil {
		return nil, err
	}
	if tx.Status == StatusSubmitted || tx.Status == StatusExecuted || tx.Status == StatusRetryable {
		return clone(tx), nil // 幂等重放（RETRYABLE 本身表示已提交、待补做）
	}
	if err := o.checkExpiry(tx); err != nil {
		return clone(tx), err
	}
	if tx.Status != StatusApproved {
		return clone(tx), fmt.Errorf("submit on %s: %w", tx.Status, ErrInvalidTransition)
	}
	if tx.Ordered() {
		// 有序交易：提交即进入 SUBMITTED 排队，不提前占用 nonce。
		tx.Status = StatusSubmitted
		return clone(tx), nil
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

// Execute 执行交易并写账本（不等待更小序号的有序交易）。
//
// 无序号老交易：余额不足时进入 FAILED 终态且 nonce 已消耗，不会部分扣款；
// 对终态（EXECUTED/FAILED）重放是幂等 no-op。
//
// 有序交易：只有当本钱包内更小执行序号的交易都进入终态后才真正落账，
// 否则返回 ErrSeqAhead（本次未落账，可稍后重试）。落账时才分配 nonce；
// 余额不足等执行失败进入 RETRYABLE（nonce 未消耗、无半截账），等待补做；
// 补做成功只扣一次钱，多次并发补做也只有一次真正落账。
func (o *Orchestrator) Execute(txID string) (*Transaction, error) {
	return o.executeWait(txID, nil)
}

// ExecuteWait 与 Execute 相同，但对有序交易允许等待更小序号走完后再执行，
// 结论与"直接轮到它执行"完全一致。ctx 被取消/超时时返回 ctx.Err()，
// 期间交易状态不被改变。传 context.Background() 表示一直等待。
func (o *Orchestrator) ExecuteWait(ctx context.Context, txID string) (*Transaction, error) {
	return o.executeWait(txID, ctx)
}

func (o *Orchestrator) executeWait(txID string, ctx context.Context) (*Transaction, error) {
	// ctx 为 nil 表示非等待模式：发现前面还有未走完的序号就立刻返回 ErrSeqAhead。
	wait := ctx != nil

	o.mu.Lock()
	defer o.mu.Unlock()
	for {
		tx, _, err := o.loadTx(txID)
		if err != nil {
			return nil, err
		}
		// 终态 / 已提交 / 可补做 的重放判定。
		switch tx.Status {
		case StatusExecuted, StatusFailed:
			return clone(tx), nil // 幂等重放：不重复扣款、状态不倒退
		case StatusRejected:
			return clone(tx), fmt.Errorf("execute on %s: %w", tx.Status, ErrInvalidTransition)
		case StatusSubmitted, StatusRetryable:
			// 继续下面的序号闸门与执行
		default:
			return clone(tx), fmt.Errorf("execute on %s: %w", tx.Status, ErrInvalidTransition)
		}

		if tx.Ordered() {
			if blocker := o.seqBlocker(tx); blocker != "" {
				if !wait {
					return clone(tx), fmt.Errorf("tx %s waiting for earlier tx %s: %w", tx.ID, blocker, ErrSeqAhead)
				}
				if err := ctx.Err(); err != nil {
					return clone(tx), err
				}
				// 释放锁等待更小序号交易的状态变化；最多等一个心跳周期，
				// 以便即使错过广播也能重新判定并及时响应 ctx 取消。
				if !waitWithTimeout(&o.mu, o.cond, waitTick, ctx) {
					return clone(tx), ctx.Err()
				}
				continue
			}
		}
		return o.runExecution(tx)
	}
}

// waitTick 是等待更小序号交易时的最长睡眠粒度。
const waitTick = 50 * time.Millisecond

// waitWithTimeout 在 cond 上等待至多 d；被 Broadcast 唤醒或超时都返回 true，
// 仅当等待期间 ctx 已结束时返回 false（调用方据此返回 ctx.Err()）。
// 调用时须持有 mu。
func waitWithTimeout(_ *sync.Mutex, c *sync.Cond, d time.Duration, ctx context.Context) bool {
	timer := time.AfterFunc(d, func() {
		c.Broadcast() // 心跳唤醒：重新判定闸门与 ctx
	})
	defer timer.Stop()
	c.Wait()
	return ctx.Err() == nil
}

// seqBlocker 返回阻止该有序交易执行的最小序号交易 ID；无阻塞时返回 ""。
// 仅同一钱包内序号更小、且尚未进入终态的有序交易构成阻塞。
func (o *Orchestrator) seqBlocker(tx *Transaction) string {
	w := o.wallets[tx.WalletID]
	var blockerSeq int64 = -1
	for seq, id := range w.seqs {
		if seq >= tx.ExecSeq {
			continue
		}
		other := o.txs[id]
		if other == nil || isTerminal(other.Status) {
			continue
		}
		if blockerSeq < 0 || seq < blockerSeq {
			blockerSeq = seq
		}
	}
	if blockerSeq < 0 {
		return ""
	}
	return w.seqs[blockerSeq]
}

func isTerminal(s TxStatus) bool {
	return s == StatusExecuted || s == StatusFailed || s == StatusRejected
}

// runExecution 执行一次真正的扣款尝试。调用方必须已通过状态与序号闸门检查。
// 成功：有序交易在此分配 nonce 并落账；失败：
//   - 无序号老交易 -> FAILED 终态（nonce 已消耗）；
//   - 有序交易     -> RETRYABLE（nonce 未消耗、无流水、可补做）。
func (o *Orchestrator) runExecution(tx *Transaction) (*Transaction, error) {
	w := o.wallets[tx.WalletID]
	nonce := tx.Nonce
	if tx.Ordered() {
		nonce = w.nextNonce // 有序交易：落账这一刻才占用 nonce
	}
	tx.Attempts++
	err := o.ledger.Post(JournalEntry{
		WalletID: tx.WalletID,
		TxID:     tx.ID,
		To:       tx.To,
		Amount:   tx.Amount,
		Nonce:    nonce,
		ExecSeq:  tx.ExecSeq,
	})
	if err != nil {
		tx.FailReason = classifyExecError(err).Error()
		if tx.Ordered() {
			tx.Status = StatusRetryable // 不消耗 nonce，不留半截账，等待补做
			o.cond.Broadcast()          // 状态变化（失败本身不解除闸门，仅通知重判）
			return clone(tx), classifyExecError(err)
		}
		tx.Status = StatusFailed
		o.cond.Broadcast() // 老交易失败也是终态：解除它作为更小序号时不会发生（无序号）
		return clone(tx), classifyExecError(err)
	}
	if tx.Ordered() {
		w.usedNonces[nonce] = tx.ID
		w.nextNonce++
		tx.Nonce = nonce
	}
	tx.FailReason = ""
	tx.Status = StatusExecuted
	o.cond.Broadcast() // 唤醒等待更大序号的交易
	return clone(tx), nil
}

// classifyExecError 将账本层错误归到稳定的业务错误类别。
func classifyExecError(err error) error {
	if err == ErrInsufficientFunds {
		return ErrInsufficientFunds
	}
	return err
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
		o.cond.Broadcast() // 终态变化：等待更大序号的交易可重新判定闸门
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
