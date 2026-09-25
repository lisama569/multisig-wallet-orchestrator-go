package multisig

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

// BuildRequest 是构建交易的输入。
type BuildRequest struct {
	WalletID       string
	IdempotencyKey string
	To             string
	Amount         int64
}

// Receipt 是提交（或重放）交易后的确定结论。
type Receipt struct {
	TxID     string
	WalletID string
	Nonce    uint64
	Status   TxStatus
	// Replayed=true 表示本次调用未发生新扣款，结论来自之前的执行记录。
	Replayed bool
}

// Orchestrator 编排多签交易的构建、审批、提交与恢复。
type Orchestrator struct {
	mu          sync.Mutex
	ledger      *Ledger
	store       *Store
	policyStore *PolicyStore
	policies    map[string]*Policy
	// nextNonce[walletID] 是下一个待分配的 nonce（含在途预留）。
	nextNonce map[string]uint64
	now       func() time.Time
	// txLocks 为每笔交易提供串行化提交，使同一 txID 的并发调用收敛为同一结论。
	txLocks sync.Map // txID -> *txGate
}

type txGate struct{ mu sync.Mutex }

// NewOrchestrator 在 dataDir 下打开/创建账本与状态文件，并修复在途交易状态。
func NewOrchestrator(dataDir string) (*Orchestrator, error) {
	ledger, err := NewLedger(filepath.Join(dataDir, "ledger.json"))
	if err != nil {
		return nil, err
	}
	store, err := NewStore(filepath.Join(dataDir, "transactions.json"))
	if err != nil {
		return nil, err
	}
	policyStore, err := NewPolicyStore(filepath.Join(dataDir, "policies.json"))
	if err != nil {
		return nil, err
	}
	o := &Orchestrator{
		ledger:      ledger,
		store:       store,
		policyStore: policyStore,
		policies:    policyStore.All(),
		nextNonce:   map[string]uint64{},
		now:         time.Now,
	}
	o.rebuildNonceCursor()
	if err := o.recoverInFlight(); err != nil {
		return nil, err
	}
	return o, nil
}

// SetClock 注入时钟，仅供测试控制交易有效期。
func (o *Orchestrator) SetClock(now func() time.Time) {
	if now != nil {
		o.now = now
	}
}

// rebuildNonceCursor 依据已持久化交易（含在途）与账本重建 nonce 分配游标，
// 取“已分配最大 nonce + 1”与“账本已执行最大 nonce + 1”的较大者。
func (o *Orchestrator) rebuildNonceCursor() {
	o.mu.Lock()
	defer o.mu.Unlock()
	walletSet := map[string]struct{}{}
	for _, w := range o.store.listWallets() {
		walletSet[w] = struct{}{}
	}
	for w := range o.policies {
		walletSet[w] = struct{}{}
	}
	for walletID := range walletSet {
		var maxAssigned uint64
		var found bool
		for _, tx := range o.store.ListByWallet(walletID) {
			if !found || tx.Nonce > maxAssigned {
				maxAssigned, found = tx.Nonce, true
			}
		}
		var cursor uint64
		if found {
			cursor = maxAssigned + 1
		}
		if last, ok := o.ledger.LastNonce(walletID); ok && last+1 > cursor {
			cursor = last + 1
		}
		o.nextNonce[walletID] = cursor
	}
}

// AddPolicy 注册（或替换）钱包授权策略并持久化。
func (o *Orchestrator) AddPolicy(p *Policy) error {
	if err := o.policyStore.Put(p); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.policies[p.WalletID] = p
	// 注册策略后也确保 nonce 游标考虑账本历史。
	if last, ok := o.ledger.LastNonce(p.WalletID); ok {
		if last+1 > o.nextNonce[p.WalletID] {
			o.nextNonce[p.WalletID] = last + 1
		}
	}
	return nil
}

// Deposit 为账户充值，便于本地演示与测试。
func (o *Orchestrator) Deposit(account string, amount int64) error {
	return o.ledger.Credit(account, amount)
}

// Balance 查询账户余额。
func (o *Orchestrator) Balance(account string) int64 { return o.ledger.Balance(account) }

// BuildTx 构建交易并分配 nonce；相同幂等键的重复/并发请求返回同一交易。
func (o *Orchestrator) BuildTx(req BuildRequest) (*Transaction, error) {
	policy := o.requirePolicy(req.WalletID)
	if policy == nil {
		return nil, NewWalletError(ErrCodeNotFound, "wallet policy not registered: "+req.WalletID)
	}
	if req.IdempotencyKey == "" {
		return nil, NewWalletError(ErrCodeInvalidArgument, "idempotency key must not be empty")
	}
	if req.To == "" {
		return nil, NewWalletError(ErrCodeInvalidArgument, "payee must not be empty")
	}
	if req.Amount <= 0 {
		return nil, NewWalletError(ErrCodeInvalidArgument, "amount must be positive")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// 幂等：相同（钱包，幂等键）始终返回首次构建的交易，不重复分配 nonce。
	if existing, ok := o.store.GetByIdempotencyKey(req.WalletID, req.IdempotencyKey); ok {
		return existing, nil
	}

	nonce := o.nextNonce[req.WalletID]
	sum := sha256.Sum256([]byte(req.WalletID + ":" + req.IdempotencyKey))
	now := o.now()
	tx := &Transaction{
		ID:             fmt.Sprintf("%s-%d-%s", req.WalletID, nonce, hex.EncodeToString(sum[:6])),
		WalletID:       req.WalletID,
		IdempotencyKey: req.IdempotencyKey,
		Nonce:          nonce,
		To:             req.To,
		Amount:         req.Amount,
		CreatedAt:      now,
		ExpiresAt:      now.Add(policy.TTL()),
		Status:         StatusPending,
		Approvals:      map[string]time.Time{},
	}
	if err := o.store.Insert(tx); err != nil {
		return nil, err
	}
	o.nextNonce[req.WalletID] = nonce + 1
	return tx.Clone(), nil
}

// Approve 由签名方对交易追加审批。
//
// 采用“读取-判定-CAS”循环：两个不同签名方并发审批时，后写入者 CAS 失败后
// 重读最新状态并重判；只要自己的审批仍然有效且未重复，就会在新版本上成功，
// 不会因为他人的并发审批而被误拒。重复审批、越权、过期、终态等结论对重判稳定。
func (o *Orchestrator) Approve(txID, signer string) (*Transaction, error) {
	for attempt := 0; ; attempt++ {
		tx, ok := o.store.Get(txID)
		if !ok {
			return nil, NewWalletError(ErrCodeNotFound, "transaction not found: "+txID)
		}
		o.mu.Lock()
		policy := o.policies[tx.WalletID]
		o.mu.Unlock()
		if policy == nil {
			return nil, NewWalletError(ErrCodeNotFound, "wallet policy not registered: "+tx.WalletID)
		}

		// 终态与提交中的交易不允许审批（非法状态跃迁）。
		switch tx.Status {
		case StatusPending, StatusReady:
		default:
			return nil, NewWalletError(ErrCodeInvalidTransition,
				"approval rejected: transaction "+txID+" is in terminal/in-flight status "+string(tx.Status))
		}

		// 有效期优先判定：过期则落入终态 expired，再拒绝。
		if tx.IsExpired(o.now()) {
			return o.expireTx(tx)
		}

		if !policy.IsSigner(signer) {
			return nil, NewWalletError(ErrCodeUnauthorizedApprover,
				"signer "+signer+" is not authorized by wallet "+tx.WalletID)
		}
		if _, dup := tx.Approvals[signer]; dup {
			return nil, NewWalletError(ErrCodeDuplicateApproval,
				"signer "+signer+" already approved transaction "+txID)
		}

		updated := tx.Clone()
		updated.Approvals[signer] = o.now()
		if updated.ApprovalCount() >= policy.Threshold() && updated.Status == StatusPending {
			updated.Status = StatusReady
		}
		saved, err := o.store.CompareAndSwap(updated, tx.Version)
		if err == nil {
			return saved, nil
		}
		// 仅对版本冲突重试；其他错误（如内部 IO）直接返回。
		if ErrorCodeOf(err) != ErrCodeNonceConflict {
			return nil, err
		}
		if attempt >= 16 {
			return nil, err
		}
	}
}

// Submit 校验策略并提交执行；同一交易的并发调用收敛为同一结论。
func (o *Orchestrator) Submit(txID string) (*Receipt, error) {
	gate := o.gateFor(txID)
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return o.submitSerial(txID)
}

// submitSerial 在每笔交易的串行门内执行，调用前须持有该交易的 gate 锁。
func (o *Orchestrator) submitSerial(txID string) (*Receipt, error) {
	tx, ok := o.store.Get(txID)
	if !ok {
		return nil, NewWalletError(ErrCodeNotFound, "transaction not found: "+txID)
	}
	o.mu.Lock()
	policy := o.policies[tx.WalletID]
	o.mu.Unlock()
	if policy == nil {
		return nil, NewWalletError(ErrCodeNotFound, "wallet policy not registered: "+tx.WalletID)
	}

	// 幂等快路径：账本是“是否扣款”的唯一事实来源。
	// 无论交易状态停在何处，只要账本已有记录，就返回同一结论且不再扣款。
	if appliedNonce, applied := o.ledger.AppliedNonce(txID); applied {
		fixed, err := o.repairStatus(tx, StatusExecuted)
		if err == nil {
			tx = fixed
		}
		return &Receipt{TxID: txID, WalletID: tx.WalletID, Nonce: appliedNonce,
			Status: StatusExecuted, Replayed: true}, nil
	}

	// 状态门禁。
	switch tx.Status {
	case StatusPending, StatusReady:
		// 可提交阶段，继续校验。
	case StatusSubmitted:
		// 恢复后不应再出现（构造时已修复）；防御性地按在途失败处理。
		return nil, NewWalletError(ErrCodeInvalidTransition,
			"transaction "+txID+" is still submitting; retry after recovery")
	case StatusFailed:
		return nil, NewWalletError(ErrCodeInvalidTransition,
			"transaction "+txID+" previously failed and must be explicitly retried")
	case StatusExpired:
		return nil, NewWalletError(ErrCodeExpired, "transaction "+txID+" has expired")
	case StatusExecuted:
		// 账本无记录但状态已执行属数据损坏，不做静默容忍。
		return nil, NewWalletError(ErrCodeInternal,
			"transaction "+txID+" marked executed but missing ledger record")
	}

	// 有效期校验。
	if tx.IsExpired(o.now()) {
		if _, err := o.expireTx(tx); err != nil {
			return nil, err
		}
		return nil, NewWalletError(ErrCodeExpired, "transaction "+txID+" has expired")
	}

	// 阈值校验。
	if tx.ApprovalCount() < policy.Threshold() {
		return nil, NewWalletError(ErrCodePolicyNotSatisfied,
			fmt.Sprintf("transaction %s requires %d approvals, got %d",
				txID, policy.Threshold(), tx.ApprovalCount()))
	}

	// nonce 顺序校验：必须恰好是钱包下一个可执行 nonce。
	// 与到达顺序无关——未轮到的交易无论何时提交都会被明确拒绝。
	last, hasLast := o.ledger.LastNonce(tx.WalletID)
	var expectedNonce uint64
	if hasLast {
		expectedNonce = last + 1
	}
	if tx.Nonce != expectedNonce {
		return nil, NewWalletError(ErrCodeNonceNotNext,
			fmt.Sprintf("transaction %s holds nonce %d, but wallet %s next executable nonce is %d",
				txID, tx.Nonce, tx.WalletID, expectedNonce))
	}

	// 跃迁为在途并持久化，先落状态再动账。
	submitted := tx.Clone()
	ts := o.now()
	submitted.SubmittedAt = &ts
	submitted.Status = StatusSubmitted
	saved, err := o.store.CompareAndSwap(submitted, tx.Version)
	if err != nil {
		return nil, err
	}
	tx = saved

	// 执行扣款。Debit 内部原子完成“余额/已执行表/nonce 游标 + 落盘”，
	// 中途失败时其自身内存也回滚，因此不会产生半笔账。
	if err := o.ledger.Debit(tx.WalletID, tx.Nonce, tx.ID, tx.To, tx.Amount); err != nil {
		// 执行失败：跃迁为 failed 并保留可区分原因，等待显式 Retry。
		failed := tx.Clone()
		failed.Status = StatusFailed
		failed.FailureCode = ErrorCodeOf(err)
		failed.FailureMessage = err.Error()
		if _, casErr := o.store.CompareAndSwap(failed, tx.Version); casErr != nil {
			// CAS 失败说明状态被并发改动；返回原始执行错误，账本仍为唯一真相。
			return nil, err
		}
		return nil, err
	}

	// 扣款成功：跃迁终态 executed。
	executed := tx.Clone()
	execTime := o.now()
	executed.ExecutedAt = &execTime
	executed.Status = StatusExecuted
	done, casErr := o.store.CompareAndSwap(executed, tx.Version)
	if casErr != nil {
		// 扣款已落账即视为成功，状态修复留给重放/重载，不回报告警。
		return &Receipt{TxID: txID, WalletID: tx.WalletID, Nonce: tx.Nonce,
			Status: StatusExecuted, Replayed: false}, nil
	}
	return &Receipt{TxID: done.ID, WalletID: done.WalletID, Nonce: done.Nonce,
		Status: done.Status, Replayed: false}, nil
}

// Retry 将失败交易复位为可提交状态（显式恢复）。
func (o *Orchestrator) Retry(txID string) (*Transaction, error) {
	tx, ok := o.store.Get(txID)
	if !ok {
		return nil, NewWalletError(ErrCodeNotFound, "transaction not found: "+txID)
	}
	if tx.Status != StatusFailed {
		return nil, NewWalletError(ErrCodeInvalidTransition,
			"only failed transactions can be retried, current status: "+string(tx.Status))
	}
	if tx.IsExpired(o.now()) {
		return nil, NewWalletError(ErrCodeExpired, "transaction "+txID+" has expired and cannot be retried")
	}
	tx.Status = StatusReady
	tx.FailureCode = ""
	tx.FailureMessage = ""
	saved, err := o.store.CompareAndSwap(tx, tx.Version)
	if err != nil {
		return nil, err
	}
	return saved, nil
}

// Get 查询交易的当前状态（深拷贝）。
func (o *Orchestrator) Get(txID string) (*Transaction, error) {
	tx, ok := o.store.Get(txID)
	if !ok {
		return nil, NewWalletError(ErrCodeNotFound, "transaction not found: "+txID)
	}
	return tx, nil
}

// requirePolicy 取钱包策略（调用方自行判 nil）。
func (o *Orchestrator) requirePolicy(walletID string) *Policy {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.policies[walletID]
}

func (o *Orchestrator) gateFor(txID string) *txGate {
	v, _ := o.txLocks.LoadOrStore(txID, &txGate{})
	return v.(*txGate)
}

// expireTx 将可过期状态的交易 CAS 到 expired 终态，并返回可区分的过期错误。
func (o *Orchestrator) expireTx(tx *Transaction) (*Transaction, error) {
	expired := tx.Clone()
	expired.Status = StatusExpired
	if _, err := o.store.CompareAndSwap(expired, tx.Version); err != nil {
		return nil, err
	}
	return nil, NewWalletError(ErrCodeExpired,
		"transaction "+tx.ID+" expired at "+tx.ExpiresAt.Format(time.RFC3339))
}

// repairStatus 在重放路径上把交易状态修复到目标状态（仅允许合法跃迁）。
func (o *Orchestrator) repairStatus(tx *Transaction, target TxStatus) (*Transaction, error) {
	if tx.Status == target {
		return tx, nil
	}
	if !canTransition(tx.Status, target) {
		return nil, NewWalletError(ErrCodeInvalidTransition,
			"cannot recover status "+string(tx.Status)+" to "+string(target))
	}
	fixed := tx.Clone()
	fixed.Status = target
	return o.store.CompareAndSwap(fixed, tx.Version)
}

// recoverInFlight 在加载后修复在途交易：
//   - 状态为 submitted 而账本已扣款 -> 修复为 executed（扣款后的状态推进丢失）；
//   - 状态为 submitted 而账本无记录 -> 修复为 failed（扣款未发生，可显式 Retry）。
func (o *Orchestrator) recoverInFlight() error {
	// 遍历所有钱包：存储层未提供全量列举，利用 nextNonce 初始化前的全钱包扫描。
	walletSet := map[string]struct{}{}
	// 从持久化文件中无法直接按未知钱包列举，借助 ListByWallet 对已知钱包补扫；
	// 同时对所有可能遗留 submitted 的钱包，通过 store 的全量快照扫描。
	for _, walletID := range o.store.listWallets() {
		walletSet[walletID] = struct{}{}
	}
	for walletID := range walletSet {
		for _, tx := range o.store.ListByWallet(walletID) {
			if tx.Status != StatusSubmitted {
				continue
			}
			if _, applied := o.ledger.AppliedNonce(tx.ID); applied {
				fixed := tx.Clone()
				fixed.Status = StatusExecuted
				if _, err := o.store.CompareAndSwap(fixed, tx.Version); err != nil {
					return err
				}
			} else {
				fixed := tx.Clone()
				fixed.Status = StatusFailed
				fixed.FailureCode = ErrCodeInternal
				fixed.FailureMessage = "recovered: submission interrupted before ledger commit"
				if _, err := o.store.CompareAndSwap(fixed, tx.Version); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
