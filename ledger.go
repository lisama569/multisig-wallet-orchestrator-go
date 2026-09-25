package multisig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ledgerSnapshot 是 Ledger 的 JSON 持久化格式。
type ledgerSnapshot struct {
	Balances     map[string]int64        `json:"balances"`
	Applied      map[string]appliedEntry `json:"applied"`
	WalletNonces map[string]uint64       `json:"wallet_nonces"`
}

// Ledger 是本地账本：维护账户余额，并记录每个钱包已执行的 nonce 与 txID，
// 为重放提供“是否已经扣款”的唯一事实来源。
type Ledger struct {
	mu       sync.Mutex
	path     string
	balances map[string]int64
	// applied[txID] = nonce：已成功扣款的交易。
	applied map[string]appliedEntry
	// walletNonces[walletID] = 已执行的最大 nonce。
	walletNonces map[string]uint64
}

type appliedEntry struct {
	WalletID string `json:"wallet_id"`
	Nonce    uint64 `json:"nonce"`
}

// NewLedger 打开（或创建）路径上的 JSON 账本；文件已存在时加载其内容。
func NewLedger(path string) (*Ledger, error) {
	l := &Ledger{
		path:         path,
		balances:     map[string]int64{},
		applied:      map[string]appliedEntry{},
		walletNonces: map[string]uint64{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, WrapError(ErrCodeInternal, "read ledger file "+path, err)
		}
		return l, nil // 全新账本，首次写入时落盘
	}
	var snap ledgerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, WrapError(ErrCodeInternal, "parse ledger file "+path, err)
	}
	if snap.Balances != nil {
		l.balances = snap.Balances
	}
	if snap.Applied != nil {
		l.applied = snap.Applied
	}
	if snap.WalletNonces != nil {
		l.walletNonces = snap.WalletNonces
	}
	return l, nil
}

// Credit 向账户入账（仅用于充值与收款）。
func (l *Ledger) Credit(account string, amount int64) error {
	if account == "" {
		return NewWalletError(ErrCodeInvalidArgument, "account must not be empty")
	}
	if amount <= 0 {
		return NewWalletError(ErrCodeInvalidArgument, "credit amount must be positive")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.balances[account] += amount
	return l.persistLocked()
}

// Balance 读取账户余额。
func (l *Ledger) Balance(account string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[account]
}

// AppliedNonce 返回某笔交易是否已扣款及其 nonce。
func (l *Ledger) AppliedNonce(txID string) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.applied[txID]
	return e.Nonce, ok
}

// LastNonce 返回钱包已执行的最大 nonce；无记录时返回 0,false。
func (l *Ledger) LastNonce(walletID string) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, ok := l.walletNonces[walletID]
	return n, ok
}

// Debit 尝试对一笔交易扣款，必须满足三个条件，任一不满足都明确拒绝：
//  1. 同一 txID 未扣过款（重放返回幂等成功由调用方依据 AppliedNonce 判定，这里拒绝重复扣减）；
//  2. nonce 必须严格等于“钱包已执行最大 nonce + 1”，重复/跳号/回退一律拒绝；
//  3. 付款方（钱包）余额充足。
//
// 三处状态（已执行表、nonce 游标、余额）在同一把锁内与持久化原子更新：
// 持久化失败则全部不落，状态不会出现“扣了款但没记账”的中途态。
func (l *Ledger) Debit(walletID string, nonce uint64, txID, to string, amount int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if amount < 0 {
		return NewWalletError(ErrCodeInvalidArgument, "debit amount must not be negative")
	}
	if _, dup := l.applied[txID]; dup {
		return NewWalletError(ErrCodeNonceConflict,
			"transaction "+txID+" already applied to ledger, duplicate debit rejected")
	}
	// 空钱包首笔 nonce 必须为 0；之后必须严格等于已执行最大 nonce + 1。
	last, hasLast := l.walletNonces[walletID]
	want := uint64(0)
	if hasLast {
		want = last + 1
	}
	if nonce != want {
		return NewWalletError(ErrCodeNonceNotNext,
			"wallet "+walletID+" expects nonce "+strconv.FormatUint(want, 10)+" but got "+strconv.FormatUint(nonce, 10))
	}
	bal := l.balances[walletID]
	if bal < amount {
		return NewWalletError(ErrCodeInsufficientBalance,
			"wallet "+walletID+" balance "+strconv.FormatInt(bal, 10)+" < debit "+strconv.FormatInt(amount, 10))
	}

	// 在内存中应用全部变更。
	l.balances[walletID] = bal - amount
	if amount > 0 && to != "" {
		l.balances[to] += amount
	}
	l.applied[txID] = appliedEntry{WalletID: walletID, Nonce: nonce}
	l.walletNonces[walletID] = nonce

	// 原子落盘；失败则回滚内存变更，保证内存与磁盘一致。
	if err := l.persistLocked(); err != nil {
		delete(l.applied, txID)
		delete(l.walletNonces, walletID)
		l.balances[walletID] = bal
		if amount > 0 && to != "" {
			l.balances[to] -= amount
		}
		return err
	}
	return nil
}

// persistLocked 将当前状态原子写入文件（同目录临时文件 + rename）。调用方须持有 l.mu。
func (l *Ledger) persistLocked() error {
	snap := ledgerSnapshot{
		Balances:     l.balances,
		Applied:      l.applied,
		WalletNonces: l.walletNonces,
	}
	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return WrapError(ErrCodeInternal, "marshal ledger snapshot", err)
	}
	if dir := filepath.Dir(l.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return WrapError(ErrCodeInternal, "create ledger dir "+dir, err)
		}
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return WrapError(ErrCodeInternal, "write ledger tmp file", err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return WrapError(ErrCodeInternal, "rename ledger tmp file", err)
	}
	return nil
}
