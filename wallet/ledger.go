package wallet

// Ledger 是本地账本，记录各地址余额。
// 账本本身不加锁，并发安全由 Orchestrator 的互斥锁保证。
type Ledger struct {
	balances map[string]int64
}

// NewLedger 创建空账本。
func NewLedger() *Ledger {
	return &Ledger{balances: make(map[string]int64)}
}

// Balance 返回地址余额。
func (l *Ledger) Balance(addr string) int64 {
	return l.balances[addr]
}

// Credit 向地址入账。
func (l *Ledger) Credit(addr string, amount int64) {
	l.balances[addr] += amount
}

// Debit 从地址出账，余额不足时返回 ErrInsufficientFunds 且不改动余额。
func (l *Ledger) Debit(addr string, amount int64) error {
	if l.balances[addr] < amount {
		return ErrInsufficientFunds
	}
	l.balances[addr] -= amount
	return nil
}
