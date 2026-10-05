# multisig-wallet-orchestrator-go

本地多签钱包的交易编排与授权流程实现。用本地账本（`Ledger`）与内存状态模拟签名方、审批策略与交易提交，保证并发提交与重复请求下结论一致且可解释。无外部依赖，仅使用 Go 标准库。

## 结构

```
wallet/
  types.go          交易、策略、状态枚举
  errors.go         可区分的哨兵错误
  ledger.go         本地账本（余额）
  orchestrator.go   编排器：提案/审批/提交/执行/放弃，nonce 与执行序号管理、失败补做
  snapshot.go       状态快照与恢复（重放验证）
  orchestrator_test.go
  seq_retry_test.go 执行序号、冲突、失败恢复、并发补做、重载续跑用例
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
PENDING --(审批达阈值)--> APPROVED --(Submit)--> SUBMITTED --(Execute 成功)------> EXECUTED
   |                        |                        |
   |                        |                        +--(余额不足等可恢复失败)--> RETRYABLE
   |                        |                                       |
   |                        |                                       +--(条件满足后 Execute)--> EXECUTED
   |                        |                                       +--(Abandon)-------------> FAILED
   |                        |                                       +--(再失败)--> RETRYABLE（结论稳定）
   |                        |
   |                        +--(Abandon)--> FAILED
   |                        |
   +--(过期)--> REJECTED <--+
```

非法跃迁（如对 `PENDING` 直接 `Execute`）被拒绝并返回 `ErrInvalidTransition`，状态不被污染。
`EXECUTED`/`FAILED`/`REJECTED` 为终态；`RETRYABLE` 不是终态，表示"还能再救一次"。

## 业务执行序号（seq）

`ProposeWithSeq(idemKey, walletID, to, amount, seq)` 在提案时携带业务侧约定的执行序号：

- **同一钱包内序号唯一**：seq 已被占用时，后到的提案被**明确拒绝**并返回 `ErrSeqConflict`，
  调用方一眼能看出是序号被占；交易不会被悄悄替换、合并或另派号码。并发争抢同一序号时，
  恰好一个提案成功，其余全部得到 `ErrSeqConflict`。
- **序号一经占用即永久绑定该交易**：即使交易后来被拒（`REJECTED`）或被放弃（`FAILED`），
  seq 也不释放、不回收，避免重启或补做后历史结论被静默改写。
- **落账顺序由 seq 决定，与请求到达顺序无关**：执行时若同钱包内还有更小序号的在途交易
  （未到终态），本笔返回 `ErrSeqOutOfOrder` 并保持 `SUBMITTED`，稍后重试即可；
  8 路并发、几十笔一起上，最终落账顺序与账本仍然确定、可复现。过期未走完的前序交易会在
  闸门检查时进入 `REJECTED`，不会永久堵队。
- **不要求序号连续**：seq 只决定先后，不强制紧挨；缺口不影响已有交易执行。
- seq 必须为正数，否则返回 `ErrInvalidSeq`。

## 失败与恢复语义

执行失败（当前主要是余额不足 `ErrInsufficientFunds`）时：

- 交易停在 **`RETRYABLE`**（非终态），而不是一锤子买卖：出账要么整笔成功，要么余额原样保留，
  **失败尝试不留下半截账**；
- 待余额补上或条件满足后，再次 `Execute` 即可补做：`RETRYABLE -> EXECUTED`，补做**恰好扣一次钱**；
- 补做仍失败时结论稳定：仍是 `RETRYABLE`，失败原因可通过 `FailKind`（哨兵错误分类）/
  `FailReason`（可读描述）区分是哪一类，重试次数记录在 `Retries`；
- 不想再救可调用 `Abandon`：`SUBMITTED`/`RETRYABLE -> FAILED`（终态），nonce 已消耗且绝不扣款；
  对终态交易再 `Execute` 返回 `ErrNotRetryable`；
- **重启/重载后续跑**：`Snapshot`/`Restore` 会带走序号占用表、nonce 游标与各交易状态，
  `RETRYABLE` 交易在新实例上补钱后继续补做，顺序闸门依旧生效，已落账的不重复扣款。

并发补做语义：同一笔尚未成功的交易被多个客户端同时 `Execute`，所有状态变更由同一把互斥锁
串行化，因此**只能有一次真正落账**；成功者落账，其余调用拿到与首次一致的 `EXECUTED` 结果，
状态不倒退、不跳步、不重复扣钱。已经 `EXECUTED` 的交易再次发起仍是幂等 no-op。

## 状态查询

- `GetTx(id)` / `ListTxs(walletID)` 可随时查询每笔交易走到了哪一步；
- `Transaction.Settled()`（是否已落账）、`Terminal()`（是否终态）、`Retryable()`（还能否补做）
  以及 `Status`/`FailKind`/`FailReason`/`Retries` 字段给出完整判定信息；
- `ListTxs` 按 seq 升序返回，并发压测后逐笔与账本核对（成功笔数 × 金额 == 收款方入账）。

## 与老行为的兼容范围

- 老的 `Propose(idemKey, walletID, to, amount)` 签名保持不变，等价于 seq=0：**不参与**
  序号占用与顺序闸门，行为与旧版一致（内部 nonce 游标语义不变）；
- 幂等键去重、审批/重复审批规则、过期拒绝、非法跃迁拒绝、快照恢复等既有行为均不变；
- **一处语义变化**：余额不足导致的执行失败不再直接进入终态 `FAILED`，而是进入可补做的
  `RETRYABLE`；需要"不可恢复失败"语义时，对该交易调用 `Abandon`。已完成交易的重放
  仍是幂等 no-op。建议同一钱包统一使用带 seq 或不带 seq 两种模式之一，不建议混用做顺序假设。

## nonce 与并发语义

- nonce 在 `Submit` 时从钱包的单调游标分配（内部记账游标），编排器内所有状态变更由同一把互斥锁串行化，因此并发提交不会产生重复 nonce 或相互覆盖；
- 同一交易被并发重复 `Submit` 是幂等 no-op（含 `RETRYABLE`），返回首次分配的结果；
- 失败/放弃的交易**消耗**其 nonce（类似以太坊语义），后续交易使用下一个 nonce，保证在途交易不会复用已占用序号；
- 业务落账顺序由 seq 闸门保证（见上）；不携带 seq 的老用法下，每笔交易仍得到唯一 nonce，结论确定。

## 幂等与失败恢复

- `Propose`/`ProposeWithSeq` 按 `IdempotencyKey` 去重：相同键重复提案（包括并发）返回首次创建的交易；
- `Execute` 对 `EXECUTED` 的重放是幂等 no-op：不重复扣款、状态不倒退；`Submit` 对已提交（含 `RETRYABLE`）交易同样幂等；
- 执行失败（如余额不足）不会部分扣款：出账要么全部成功，要么原样保留，交易停在 `RETRYABLE` 等待补做；
- `Abandon` 显式放弃补做，交易进入 `FAILED` 终态（nonce 已消耗、不扣款）；
- `Snapshot`/`Restore` 可导出并重建全部状态（账本、交易、nonce 游标、幂等键索引、**执行序号占用表**），恢复后可继续补做未走完的交易，重放已完成交易结论与原实例一致。

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
| `ErrInsufficientFunds` | 余额不足（可恢复，交易进入 `RETRYABLE`） |
| `ErrInvalidPolicy` | 策略不合法（阈值越界、签名方为空或重复、TTL 非正） |
| `ErrInvalidAmount` | 金额不合法 |
| `ErrSeqConflict` | 同一钱包内执行序号已被另一笔交易占用 |
| `ErrInvalidSeq` | 执行序号不合法（<=0） |
| `ErrSeqOutOfOrder` | 更小序号的在途交易尚未走完，本笔暂不能执行（稍后重试） |
| `ErrNotRetryable` | 交易已到不可补做的终态（`FAILED`/`REJECTED`） |

均可用 `errors.Is` 判定；最近一次失败的分类同时写入 `Transaction.FailKind`。

## 本地验证

```sh
go build ./...
go vet ./...
go test ./... -v        # 日志打印关键输入（到达顺序/序号/余额/尝试次数）与判定结果
go test ./... -race     # 并发竞态检测
```

原有覆盖（审批/越权/重复审批、同键并发提案去重、过期拒绝、非法跃迁、nonce 唯一、
快照恢复重放、并发全流程终态确定）保持不回归；新增覆盖：

- `TestSeqOrderIndependentOfArrival`：40 笔交易乱序到达 + 8 路并发，最终严格按 seq 落账，账本确定；
- `TestSeqConflictRejected`：重复/并发争抢同一 seq 被明确拒绝，序号占用不释放；
- `TestLegacyProposeWithoutSeqStillWorks`：不带序号老用法与新机制互不干扰；
- `TestRetryAfterFundingSucceeds`：余额不足进 `RETRYABLE`，补钱后补做成功且恰好扣一次；
- `TestConcurrentRetrySettlesOnce`：多客户端并发重新执行，只落账一次、结论一致；
- `TestRestoreThenResumeInFlight`：快照重载后续跑 `RETRYABLE` 交易，序号/nonce/顺序闸门延续；
- `TestQueryConsistentWithLedgerAfterConcurrentRound`：并发压测后逐笔查询与账本一致，补钱后全部救活。
