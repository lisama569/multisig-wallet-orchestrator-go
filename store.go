package multisig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// storeSnapshot 是 Store 的 JSON 持久化格式。
type storeSnapshot struct {
	Transactions []*Transaction               `json:"transactions"`
	IdemIndex    map[string]map[string]string `json:"idem_index"`
}

// Store 是交易状态的持久化存储：内存索引 + JSON 快照，
// 用 Version 做乐观并发控制，防止并发提交相互覆盖。
type Store struct {
	mu        sync.Mutex
	path      string
	txs       map[string]*Transaction
	idemIndex map[string]map[string]string
}

// NewStore 打开（或创建）路径上的 JSON 状态文件；文件已存在时加载。
func NewStore(path string) (*Store, error) {
	s := &Store{
		path:      path,
		txs:       map[string]*Transaction{},
		idemIndex: map[string]map[string]string{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, WrapError(ErrCodeInternal, "read store file "+path, err)
		}
		return s, nil
	}
	var snap storeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, WrapError(ErrCodeInternal, "parse store file "+path, err)
	}
	for _, tx := range snap.Transactions {
		if tx.Approvals == nil {
			tx.Approvals = map[string]time.Time{}
		}
		s.txs[tx.ID] = tx
	}
	if snap.IdemIndex != nil {
		s.idemIndex = snap.IdemIndex
	}
	return s, nil
}

// Get 返回交易的深拷贝；不存在时 ok=false。
func (s *Store) Get(txID string) (*Transaction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, ok := s.txs[txID]
	if !ok {
		return nil, false
	}
	return tx.Clone(), true
}

// GetByIdempotencyKey 按幂等键查找交易（同一钱包内唯一）。
func (s *Store) GetByIdempotencyKey(walletID, key string) (*Transaction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.idemIndex[walletID][key]
	if !ok {
		return nil, false
	}
	return s.txs[id].Clone(), true
}

// Insert 写入一笔新交易；txID 或幂等键冲突时返回 ErrCodeNonceConflict。
func (s *Store) Insert(tx *Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.txs[tx.ID]; exists {
		return NewWalletError(ErrCodeNonceConflict, "transaction id "+tx.ID+" already exists")
	}
	idx := s.idemIndex[tx.WalletID]
	if idx == nil {
		idx = map[string]string{}
		s.idemIndex[tx.WalletID] = idx
	}
	if tx.IdempotencyKey != "" {
		if existing, dup := idx[tx.IdempotencyKey]; dup {
			return NewWalletError(ErrCodeNonceConflict,
				"idempotency key "+tx.IdempotencyKey+" already bound to transaction "+existing)
		}
	}
	stored := tx.Clone()
	stored.Version = 1
	s.txs[stored.ID] = stored
	idx[stored.IdempotencyKey] = stored.ID
	return s.persistLocked()
}

// CompareAndSwap 仅当库中 Version == expectedVersion 时写入并递增版本，
// 否则返回 ErrCodeNonceConflict 及库中最新拷贝，调用方必须重读后决定重试或拒绝。
func (s *Store) CompareAndSwap(tx *Transaction, expectedVersion int) (*Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.txs[tx.ID]
	if !ok {
		return nil, NewWalletError(ErrCodeNotFound, "transaction "+tx.ID+" not found")
	}
	if current.Version != expectedVersion {
		return current.Clone(), NewWalletError(ErrCodeNonceConflict,
			"transaction "+tx.ID+" version changed concurrently: expected "+
				strconv.Itoa(expectedVersion)+", actual "+strconv.Itoa(current.Version))
	}
	stored := tx.Clone()
	stored.Version = current.Version + 1
	s.txs[stored.ID] = stored
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return stored.Clone(), nil
}

// listWallets 列举存储中出现过的全部钱包 ID（用于启动恢复扫描）。
func (s *Store) listWallets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]struct{}{}
	for _, tx := range s.txs {
		seen[tx.WalletID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for w := range seen {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

// ListByWallet 返回钱包内全部交易（按 nonce 升序）的深拷贝。
func (s *Store) ListByWallet(walletID string) []*Transaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Transaction
	for _, tx := range s.txs {
		if tx.WalletID == walletID {
			out = append(out, tx.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Nonce < out[j].Nonce })
	return out
}

// persistLocked 原子落盘全部交易快照。调用方须持有 s.mu。
func (s *Store) persistLocked() error {
	snap := storeSnapshot{IdemIndex: s.idemIndex}
	for _, tx := range s.txs {
		snap.Transactions = append(snap.Transactions, tx.Clone())
	}
	sort.Slice(snap.Transactions, func(i, j int) bool {
		if snap.Transactions[i].WalletID != snap.Transactions[j].WalletID {
			return snap.Transactions[i].WalletID < snap.Transactions[j].WalletID
		}
		return snap.Transactions[i].Nonce < snap.Transactions[j].Nonce
	})
	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return WrapError(ErrCodeInternal, "marshal store snapshot", err)
	}
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return WrapError(ErrCodeInternal, "create store dir "+dir, err)
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return WrapError(ErrCodeInternal, "write store tmp file", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return WrapError(ErrCodeInternal, "rename store tmp file", err)
	}
	return nil
}
