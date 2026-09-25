// Package multisig 实现本地多签钱包的交易编排与授权流程：
// 基于钱包策略判定签名方集合、审批阈值与有效期，串行分配 nonce，
// 并通过账本去重、状态版本 CAS 与幂等键保证并发与重放下结论一致。
package multisig
