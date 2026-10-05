package wallet

// Ledger 是本地账本，记录各地址余额。
// 账本本身不加锁，并发安全由 Orchestrator 的互斥锁保证。
type Ledger struct {
	balances map[string]int64
	journal  []JournalEntry
}

// JournalEntry 记录一次真正成功的落账（扣款+入账原子完成）。
// 失败的执行尝试不会产生流水，因此流水长度严格等于实际扣款次数。
type JournalEntry struct {
	WalletID string
	TxID     string
	To       string
	Amount   int64
	Nonce    int64
	ExecSeq  int64 // 0 表示无序号老交易
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

// Post 原子完成一笔扣款+入账：余额不足时不改动任何余额、不追加流水，
// 调用方可据此保证"补做只扣一次钱、失败不留半截账"。
func (l *Ledger) Post(e JournalEntry) error {
	if l.balances[e.WalletID] < e.Amount {
		return ErrInsufficientFunds
	}
	l.balances[e.WalletID] -= e.Amount
	l.balances[e.To] += e.Amount
	l.journal = append(l.journal, e)
	return nil
}

// Journal 返回落账流水的副本（顺序即真实落账顺序）。
func (l *Ledger) Journal() []JournalEntry {
	return append([]JournalEntry(nil), l.journal...)
}
