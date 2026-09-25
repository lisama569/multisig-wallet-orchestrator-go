package wallet

import "errors"

// 可区分的失败原因（哨兵错误，调用方可用 errors.Is 判定）。
var (
	ErrWalletNotFound     = errors.New("wallet not found")         // 钱包不存在
	ErrTxNotFound         = errors.New("transaction not found")    // 交易不存在
	ErrUnauthorizedSigner = errors.New("signer not in policy")     // 审批人不在策略签名方集合中
	ErrDuplicateApproval  = errors.New("duplicate approval")       // 同一签名方重复审批
	ErrTxExpired          = errors.New("transaction expired")      // 交易已过有效期
	ErrInvalidTransition  = errors.New("invalid state transition") // 非法状态跃迁
	ErrNonceConflict      = errors.New("nonce conflict")           // nonce 已被其他在途交易占用
	ErrInsufficientFunds  = errors.New("insufficient funds")       // 余额不足
	ErrInvalidPolicy      = errors.New("invalid policy")           // 策略本身不合法
	ErrInvalidAmount      = errors.New("invalid amount")           // 金额不合法
)
