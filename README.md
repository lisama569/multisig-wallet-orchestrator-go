# multisig-wallet-orchestrator-go

本地多签钱包的交易编排与授权流程实现。用本地账本（`Ledger`）与内存状态模拟签名方、审批策略与交易提交，保证并发提交与重复请求下结论一致且可解释。无外部依赖，仅使用 Go 标准库。

## 结构

```
wallet/
  types.go          交易、策略、状态枚举
  errors.go         可区分的哨兵错误
  ledger.go         本地账本（余额）
  orchestrator.go   编排器：提案/审批/提交/执行，nonce 管理
  snapshot.go       状态快照与恢复（重放验证）
  orchestrator_test.go
```

## 授权策略

每个钱包注册一个 `Policy`：

- `Signers`：授权签名方集合（非空、无重复）；
- `Threshold`：生效所需的最少审批数，须满足 `1 <= Threshold <= len(Signers)`；
- `TTL`：交易自提案起的有效期，过期交易在下次操作时被置为 `REJECTED` 终态。

审批规则：

- 审批人必须属于 `Signers`，否则返回 `ErrUnauthorizedSigner`；
- 同一签名方对同一交易只能审批一次，重复审批返回 `ErrDuplicateApproval`；
- 审批数达到 `Threshold` 时交易从 `PENDING` 进入 `APPROVED`；
- 仅 `PENDING`/`APPROVED` 状态可审批，其余状态返回 `ErrInvalidTransition`。

## 交易状态机

```
PENDING --(审批达阈值)--> APPROVED --(Submit)--> SUBMITTED --(Execute 成功)--> EXECUTED
   |                        |                            |
   |                        |                            +--(Execute 失败)--> FAILED
   +--(过期)--> REJECTED <--+
```

非法跃迁（如对 `PENDING` 直接 `Execute`）被拒绝并返回 `ErrInvalidTransition`，状态不被污染。`EXECUTED`/`FAILED`/`REJECTED` 为终态。

## nonce 与并发语义

- nonce 在 `Submit` 时从钱包的单调游标分配，编排器内所有状态变更由同一把互斥锁串行化，因此并发提交不会产生重复 nonce 或相互覆盖；
- 同一交易被并发重复 `Submit` 是幂等 no-op，返回首次分配的结果；
- 执行失败的交易**消耗**其 nonce（类似以太坊语义），后续交易使用下一个 nonce，保证在途交易不会复用已占用序号；
- 结果只取决于请求被受理的顺序：无论并发提交顺序如何，每笔交易都得到唯一 nonce，终态与总账确定。

## 幂等与失败恢复

- `Propose` 按 `IdempotencyKey` 去重：相同键重复提案（包括并发）返回首次创建的交易；
- `Execute`/`Submit` 对终态或已提交交易的重放是幂等 no-op：不重复扣款、状态不倒退；
- 执行失败（如余额不足）不会部分扣款：出账要么全部成功，要么原样保留；
- `Snapshot`/`Restore` 可导出并重建全部状态（账本、交易、nonce 游标、幂等键索引），恢复后重放同一请求结论与原实例一致。

## 错误分类

| 错误 | 含义 |
|---|---|
| `ErrWalletNotFound` | 钱包不存在 |
| `ErrTxNotFound` | 交易不存在 |
| `ErrUnauthorizedSigner` | 审批人不在策略签名方集合中 |
| `ErrDuplicateApproval` | 同一签名方重复审批 |
| `ErrTxExpired` | 交易已过有效期 |
| `ErrInvalidTransition` | 非法状态跃迁 |
| `ErrNonceConflict` | nonce 已被其他在途交易占用 |
| `ErrInsufficientFunds` | 余额不足 |
| `ErrInvalidPolicy` | 策略不合法（阈值越界、签名方为空或重复、TTL 非正） |
| `ErrInvalidAmount` | 金额不合法 |

均可用 `errors.Is` 判定。

## 本地验证

```sh
go build ./...
go vet ./...
go test ./... -v        # 日志打印关键输入与判定依据
go test ./... -race     # 并发竞态检测
```

测试覆盖：并发提交唯一 nonce、并发同键提案去重、越权/重复审批、过期拒绝、非法跃迁拒绝、执行失败消耗 nonce 且重放安全、快照恢复后重放一致、并发全流程终态确定。
