package wallet

import "encoding/json"

// snapshot 是编排器与账本状态的可序列化视图，
// 用于"状态重载后重放"场景：恢复后重放同一请求，结论必须一致。
type snapshot struct {
	Balances map[string]int64 `json:"balances"`
	Wallets  map[string]struct {
		Policy     Policy           `json:"policy"`
		NextNonce  int64            `json:"next_nonce"`
		UsedNonces map[int64]string `json:"used_nonces"`
		UsedSeqs   map[int64]string `json:"used_seqs"`
	} `json:"wallets"`
	Txs    map[string]*Transaction `json:"txs"`
	ByIdem map[string]string       `json:"by_idem"`
	TxSeq  int64                   `json:"tx_seq"`
}

// Snapshot 导出当前全部状态（账本 + 交易 + nonce 游标）。
func (o *Orchestrator) Snapshot() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := snapshot{
		Balances: make(map[string]int64, len(o.ledger.balances)),
		Wallets: make(map[string]struct {
			Policy     Policy           `json:"policy"`
			NextNonce  int64            `json:"next_nonce"`
			UsedNonces map[int64]string `json:"used_nonces"`
			UsedSeqs   map[int64]string `json:"used_seqs"`
		}, len(o.wallets)),
		Txs:    make(map[string]*Transaction, len(o.txs)),
		ByIdem: make(map[string]string, len(o.byIdem)),
		TxSeq:  o.txSeq,
	}
	for k, v := range o.ledger.balances {
		s.Balances[k] = v
	}
	for id, w := range o.wallets {
		used := make(map[int64]string, len(w.usedNonces))
		for n, t := range w.usedNonces {
			used[n] = t
		}
		seqs := make(map[int64]string, len(w.usedSeqs))
		for n, t := range w.usedSeqs {
			seqs[n] = t
		}
		s.Wallets[id] = struct {
			Policy     Policy           `json:"policy"`
			NextNonce  int64            `json:"next_nonce"`
			UsedNonces map[int64]string `json:"used_nonces"`
			UsedSeqs   map[int64]string `json:"used_seqs"`
		}{Policy: w.policy, NextNonce: w.nextNonce, UsedNonces: used, UsedSeqs: seqs}
	}
	for id, tx := range o.txs {
		s.Txs[id] = clone(tx)
	}
	for k, v := range o.byIdem {
		s.ByIdem[k] = v
	}
	return json.Marshal(s)
}

// Restore 用快照重建一个编排器；恢复后重放请求与原实例结论一致。
func Restore(data []byte) (*Orchestrator, error) {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	o := NewOrchestrator(NewLedger())
	for k, v := range s.Balances {
		o.ledger.balances[k] = v
	}
	for id, w := range s.Wallets {
		o.wallets[id] = &walletState{
			policy:     w.Policy,
			nextNonce:  w.NextNonce,
			usedNonces: w.UsedNonces,
			usedSeqs:   w.UsedSeqs,
		}
		if o.wallets[id].usedNonces == nil {
			o.wallets[id].usedNonces = make(map[int64]string)
		}
		if o.wallets[id].usedSeqs == nil {
			o.wallets[id].usedSeqs = make(map[int64]string)
		}
	}
	o.txs = s.Txs
	o.byIdem = s.ByIdem
	o.txSeq = s.TxSeq
	return o, nil
}
