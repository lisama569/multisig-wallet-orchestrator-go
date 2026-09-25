package multisig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// policyRecord 是策略的 JSON 持久化格式。
type policyRecord struct {
	WalletID  string   `json:"wallet_id"`
	Signers   []string `json:"signers"`
	Threshold int      `json:"threshold"`
	TTLMillis int64    `json:"ttl_millis"`
}

// PolicyStore 把钱包授权策略持久化到本地 JSON，使进程重载后策略仍然有效。
type PolicyStore struct {
	mu       sync.Mutex
	path     string
	policies map[string]*Policy
}

// NewPolicyStore 打开（或创建）策略文件；文件已存在时加载。
func NewPolicyStore(path string) (*PolicyStore, error) {
	s := &PolicyStore{path: path, policies: map[string]*Policy{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, WrapError(ErrCodeInternal, "read policy file "+path, err)
		}
		return s, nil
	}
	var records []policyRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, WrapError(ErrCodeInternal, "parse policy file "+path, err)
	}
	for _, r := range records {
		p, err := NewPolicy(r.WalletID, r.Signers, r.Threshold, time.Duration(r.TTLMillis)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		s.policies[p.WalletID] = p
	}
	return s, nil
}

// Get 读取一个钱包策略。
func (s *PolicyStore) Get(walletID string) (*Policy, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.policies[walletID]
	return p, ok
}

// All 返回全部已持久化策略。
func (s *PolicyStore) All() map[string]*Policy {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Policy, len(s.policies))
	for k, v := range s.policies {
		out[k] = v
	}
	return out
}

// Put 写入（或替换）策略并原子落盘。
func (s *PolicyStore) Put(p *Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policies[p.WalletID] = p
	records := make([]policyRecord, 0, len(s.policies))
	for _, v := range s.policies {
		records = append(records, policyRecord{
			WalletID:  v.WalletID,
			Signers:   v.Signers(),
			Threshold: v.threshold,
			TTLMillis: v.ttl.Milliseconds(),
		})
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return WrapError(ErrCodeInternal, "marshal policy records", err)
	}
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return WrapError(ErrCodeInternal, "create policy dir "+dir, err)
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return WrapError(ErrCodeInternal, "write policy tmp file", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return WrapError(ErrCodeInternal, "rename policy tmp file", err)
	}
	return nil
}
