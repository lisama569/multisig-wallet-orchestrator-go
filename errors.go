package multisig

import (
	"errors"
	"fmt"
)

// ErrorCode 是对所有可预期失败原因的稳定分类，调用方可以按 Code 区分处理逻辑。
type ErrorCode string

const (
	// ErrCodeNotFound 交易或钱包不存在。
	ErrCodeNotFound ErrorCode = "not_found"
	// ErrCodeInvalidArgument 请求参数非法。
	ErrCodeInvalidArgument ErrorCode = "invalid_argument"
	// ErrCodeUnauthorizedApprover 审批人不在钱包签名方集合中（越权审批）。
	ErrCodeUnauthorizedApprover ErrorCode = "unauthorized_approver"
	// ErrCodeDuplicateApproval 同一签名方对同一交易重复审批。
	ErrCodeDuplicateApproval ErrorCode = "duplicate_approval"
	// ErrCodePolicyNotSatisfied 审批数量未达到策略阈值。
	ErrCodePolicyNotSatisfied ErrorCode = "policy_not_satisfied"
	// ErrCodeExpired 交易已超过策略规定的有效期。
	ErrCodeExpired ErrorCode = "expired"
	// ErrCodeInvalidTransition 交易状态机不允许当前跃迁（终态回退、跳过阶段等）。
	ErrCodeInvalidTransition ErrorCode = "invalid_transition"
	// ErrCodeNonceConflict 并发写入导致状态版本冲突，或交易被并发修改。
	ErrCodeNonceConflict ErrorCode = "nonce_conflict"
	// ErrCodeNonceNotNext 交易 nonce 不是钱包当前可执行的最小 nonce（在途交易阻塞）。
	ErrCodeNonceNotNext ErrorCode = "nonce_not_next"
	// ErrCodeInsufficientBalance 账本余额不足，扣款被拒绝。
	ErrCodeInsufficientBalance ErrorCode = "insufficient_balance"
	// ErrCodeInternal 本地持久化等内部错误。
	ErrCodeInternal ErrorCode = "internal"
)

// WalletError 携带稳定错误码与可解释的上下文信息。
type WalletError struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *WalletError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (cause: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *WalletError) Unwrap() error { return e.Cause }

// NewWalletError 构造一个带错误码的业务错误。
func NewWalletError(code ErrorCode, message string) *WalletError {
	return &WalletError{Code: code, Message: message}
}

// WrapError 在业务错误外再包裹底层原因。
func WrapError(code ErrorCode, message string, cause error) *WalletError {
	return &WalletError{Code: code, Message: message, Cause: cause}
}

// ErrorCodeOf 提取错误中的稳定错误码；非本包错误返回 ErrCodeInternal。
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var we *WalletError
	if errors.As(err, &we) {
		return we.Code
	}
	return ErrCodeInternal
}
