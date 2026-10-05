# multisig-wallet-orchestrator-go

本地多签钱包的交易编排与授权流程实现。用本地账本（`Ledger`）与内存状态模拟签名方、审批策略与交易提交，保证并发提交与重复请求下结论一致且可解释。无外部依赖，仅使用 Go 标准库。

## 结构

```
wallet/
  types.go          交易、策略、状态枚举、状态查询辅助方法
  errors.go         可区分的哨兵错误
  ledger.go         本地账本（余额 + 落账流水）
  orchestrator.go   编排器：提案/审批/提交/执行，nonce 与执行序号管理
  snapshot.go       状态快照与恢复（重放验证）
  orchestrator_test.go
  ordered_test.go   执行序号、失败恢复、并发补做、重载续跑用例
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
   |                        |              |         |
   |                        |              |         +--(有序交易 Execute 失败)--> RETRYABLE --(Execute 补做)
   |                        |              |                                       成功--> EXECUTED
   |                        |              |                                       仍失败--> 留在 RETRYABLE
   |                        |              +--(无序号老交易 Execute 失败)--> FAILED
   +--(过期)--> REJECTED <--+
```

非法跃迁（如对 `PENDING` 直接 `Execute`）被拒绝并返回 `ErrInvalidTransition`，状态不被污染。`EXECUTED`/`FAILED`/`REJECTED` 为终态；`RETRYABLE` 不是终态，而是"执行失败、还能再救一次"的中间态。

## 执行序号（批量调度的确定性）

批量场景下多客户端几乎同时发交易，结论不能取决于请求谁先到。业务方可在提案时带一个**执行序号** `execSeq`：

- `ProposeWithSeq(idemKey, walletID, to, amount, execSeq)`：`execSeq > 0` 即有序交易；仅要求为正、不要求连续；`execSeq <= 0` 拒绝并返回 `ErrInvalidSeq`。
- **同钱包内序号唯一**：若序号已被先到的交易占用，后到者（即使是并发到达）被明确拒绝，返回带占用方交易 ID 的 `ErrSeqOccupied`。不会被替换、合并，也不会换一个别的号。
- **按序号落账，与到达顺序无关**：有序交易只有在同钱包内所有更小序号的交易都进入终态（`EXECUTED`/`FAILED`/`REJECTED`）后才真正落账。更小序号还没走完时调用 `Execute`，立即返回 `ErrSeqAhead`（本次不落账，可稍后重试）；`ExecuteWait(ctx, txID)` 则等待前面走完后自动按序执行，ctx 超时/取消时原样返回且不改变状态。
- 有序交易的 **nonce 在成功落账那一刻才分配**（与无序号老交易共用钱包的同一个单调游标），因此失败不消耗 nonce。
- 账本提供 `Ledger.Journal()` 落账流水：只有成功落账才追加一条，可直接核对"最终落账顺序 == 执行序号升序"以及"每笔只扣一次"。

## 失败与恢复语义

- 无序号老交易：`Execute` 失败（如余额不足）进入 `FAILED` 终态、nonce 已消耗、不留半截账（行为与历史版本一致）。
- 有序交易：`Execute` 失败进入 `RETRYABLE`：
  - 出账是原子的：余额不足时余额与流水都不变，**没有半截账**；
  - nonce 未消耗，状态不倒退、不跳步；
  - 失败原因分类稳定（如余额不足即 `ErrInsufficientFunds`），记录在 `Transaction.FailReason`；
  - 等余额补足或条件满足后再次 `Execute` 即可补做：成功则分配 nonce 并落账，整个生命周期**只扣这一次钱**；仍失败则继续留在 `RETRYABLE`，结论与原因不变。
- **并发补做只落账一次**：多个客户端同时对同一笔 `RETRYABLE` 交易 `Execute`，所有状态变更由同一把互斥锁串行化，只有一次真正落账，其余调用方拿到与首次一致的 `EXECUTED` 结果。`EXECUTED` 之后再次发起同样不重复生效。
- `Transaction.Attempts` 记录真实扣款尝试次数；`Journal()` 的条数严格等于成功落账次数，二者配合可核对"试了几次、只扣了一次"。

## 状态查询

`GetTx(txID)` 返回交易当前快照，配合以下判定方法随时查看进度（并发压过后仍与账本一致）：

| 方法/字段 | 含义 |
|---|---|
| `Status` | `PENDING`/`APPROVED`/`SUBMITTED`/`RETRYABLE`/`EXECUTED`/`FAILED`/`REJECTED` |
| `Posted()` | 是否已真正落账（仅 `EXECUTED`） |
| `CanRetry()` | 是否处于可补做的 `RETRYABLE` |
| `FailReason` | 最近一次失败原因（成功后清空） |
| `Attempts` | 实际扣款尝试次数 |
| `Nonce` | 落账分配到的 nonce（未分配为 -1） |

## nonce 与并发语义

- 编排器内所有状态变更由同一把互斥锁串行化；并发提交不会产生重复 nonce 或相互覆盖。
- 无序号老交易在 `Submit` 时分配 nonce；有序交易在成功落账时分配 nonce。两类交易共用同一钱包的单调 nonce 游标，混用时 nonce 全局唯一，有序交易在落账流水里的子序列仍严格按执行序号升序。
- 同一交易被并发重复 `Submit`/`Execute` 是幂等的：不重复扣款、状态不倒退。
- 无序号老交易执行失败消耗 nonce；有序交易执行失败不消耗 nonce，可补做。

## 兼容性

- `Propose(...)` 原签名与行为完全保留，等价于 `ProposeWithSeq(..., 0)`：不带序号、Submit 占 nonce、失败即 `FAILED` 终态。已有调用方无需改动。
- 同一钱包内有序与无序号交易可以混用，各自语义不变。
- 老快照缺少新增字段时也能正常恢复（新字段按零值/空索引重建）。

## 幂等、快照与失败恢复

- `Propose`/`ProposeWithSeq` 按 `IdempotencyKey` 去重：相同键重复提案（包括并发）返回首次创建的交易。
- `Execute`/`Submit` 对终态或已提交交易的重放是幂等 no-op：不重复扣款、状态不倒退。
- 执行失败不会部分扣款：出账要么全部成功，要么原样保留。
- `Snapshot`/`Restore` 导出并重建全部状态（账本、余额、**落账流水**、交易、nonce 游标、幂等键索引、**执行序号索引**）。重启/重载后：`RETRYABLE` 交易原样保留并可继续补做，已落账的不重复扣，nonce 游标与序号闸门延续、不错序。

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
| `ErrInsufficientFunds` | 余额不足（可补做交易保持 `RETRYABLE`） |
| `ErrInvalidPolicy` | 策略不合法（阈值越界、签名方为空或重复、TTL 非正） |
| `ErrInvalidAmount` | 金额不合法 |
| `ErrInvalidSeq` | 执行序号不合法（<=0） |
| `ErrSeqOccupied` | 同钱包执行序号已被先到的交易占用（后到者被明确拒绝） |
| `ErrSeqAhead` | 更小执行序号尚未走完，本次未落账，稍后可重试 |

均可用 `errors.Is` 判定。

## 本地验证

```sh
go build ./...
go vet ./...
go test ./... -v               # 日志打印每笔输入、判定结果与覆盖的业务点
go test ./... -race -count=3   # 并发竞态检测 + 多轮可复现
```

原有覆盖（不回归）：并发提交唯一 nonce、并发同键提案去重、越权/重复审批、过期拒绝、非法跃迁拒绝、执行失败消耗 nonce 且重放安全、快照恢复后重放一致、并发全流程终态确定。

新增覆盖：

- `TestOrderedExecutionIndependentOfArrival`：8 路、40 笔打乱到达顺序并发执行，落账流水严格按执行序号升序且 nonce 一一对应；
- `TestExecSeqConflictRejected`：同序号后到者拿到 `ErrSeqOccupied`（错误信息带占用方），16 路并发同序号只有 1 笔成功；
- `TestRetryableFailureThenRecover`：失败停在 `RETRYABLE`（无流水/无半截扣款/nonce 未消耗/原因可查），再失败结论稳定，补钱后补做成功且只扣一次；
- `TestConcurrentRetryPostsOnce`：32 个客户端并发补做同一笔，只落账一次、结果一致，完成后再发起不重复生效；
- `TestOrderedRetryAcrossRestore`：`RETRYABLE` 交易快照重载后仍可补做，已落账不重扣，nonce 与序号顺序延续；
- `TestExecuteWaitBlockingAndContext`：等待式执行的阻塞、ctx 超时不污染状态、唤醒后多等待者只落账一次；
- `TestMixedOrderedAndLegacyConcurrency`：有序与无序号老交易在 8 路并发中混用，两类语义互不破坏。
