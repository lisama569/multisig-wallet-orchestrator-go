package multisig

import (
	"fmt"
	"time"
)

// Policy 描述一个多签钱包的授权策略：合法签名方集合、审批阈值与交易有效期。
type Policy struct {
	WalletID  string
	signers   map[string]struct{}
	threshold int
	ttl       time.Duration
}

// NewPolicy 创建并校验钱包策略。
// 规则：钱包 ID 非空；签名方去重后至少 1 人；阈值 >=1 且不超过签名方人数；TTL > 0。
func NewPolicy(walletID string, signers []string, threshold int, ttl time.Duration) (*Policy, error) {
	if walletID == "" {
		return nil, NewWalletError(ErrCodeInvalidArgument, "wallet id must not be empty")
	}
	if len(signers) == 0 {
		return nil, NewWalletError(ErrCodeInvalidArgument, fmt.Sprintf("wallet %s requires at least one signer", walletID))
	}
	set := make(map[string]struct{}, len(signers))
	for _, s := range signers {
		if s == "" {
			return nil, NewWalletError(ErrCodeInvalidArgument, "signer address must not be empty")
		}
		set[s] = struct{}{}
	}
	if threshold < 1 {
		return nil, NewWalletError(ErrCodeInvalidArgument, fmt.Sprintf("wallet %s threshold must be >= 1, got %d", walletID, threshold))
	}
	if threshold > len(set) {
		return nil, NewWalletError(ErrCodeInvalidArgument,
			fmt.Sprintf("wallet %s threshold %d exceeds unique signer count %d", walletID, threshold, len(set)))
	}
	if ttl <= 0 {
		return nil, NewWalletError(ErrCodeInvalidArgument, fmt.Sprintf("wallet %s ttl must be positive, got %s", walletID, ttl))
	}
	return &Policy{WalletID: walletID, signers: set, threshold: threshold, ttl: ttl}, nil
}

// IsSigner 判断给定地址是否为钱包合法签名方。
func (p *Policy) IsSigner(signer string) bool {
	_, ok := p.signers[signer]
	return ok
}

// Threshold 返回策略要求的最少审批人数。
func (p *Policy) Threshold() int { return p.threshold }

// TTL 返回交易从构建起的有效时长。
func (p *Policy) TTL() time.Duration { return p.ttl }

// Signers 返回签名方地址列表（顺序不保证，仅用于持久化）。
func (p *Policy) Signers() []string {
	out := make([]string, 0, len(p.signers))
	for s := range p.signers {
		out = append(out, s)
	}
	return out
}
