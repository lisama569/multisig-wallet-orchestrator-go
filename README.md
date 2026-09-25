# multisig-wallet-orchestrator-go

本地多签钱包的**交易编排与授权流程**参考实现。用本地 JSON 账本与内存/文件状态
模拟签名方、审批策略与交易提交，目标是在**并发提交、重复请求、执行中途失败、
进程重载重放**等场景下，结论保持一致、确定、可解释，且零外部依赖（无需数据库、
网络或第三方服务）。

## 1. 快速开始

要求 Go ≥ 1.25。

```bash
go build ./...          # 编译
go vet ./...            # 静态检查
go test ./... -v        # 运行全部单测（含可运行 Example）
go test -race ./...     # 竞态检测
go test -count=10 ./... # 重复运行，验证结论可复现
```

所有数据落在一个本地目录中：

| 文件 | 内容 | 作用 |
| --- | --- | --- |
| `ledger.json` | 余额、`已执行 txID -> nonce` 表、各钱包已执行最大 nonce | “是否已经扣款”的唯一事实来源 |
| `transactions.json` | 全部交易（含审批、状态、`Version`）与幂等键索引 | 交易状态机快照 |
| `policies.json` | 钱包签名方、阈值、TTL | 授权策略持久化，重载后仍然有效 |

写入一律采用「临时文件 + `rename`」原子落盘。

最小用法见 [`example_test.go`](example_test.go)：构建 → 审批 → 提交 → 重复提交（重放）。

## 2. 授权策略（Policy）

每个钱包注册一份策略：

- **签名方集合 `signers`**：仅集合内地址可审批；集合去重。
- **审批阈值 `threshold`**：`1 <= threshold <= 去重后签名方人数`。
- **有效期 `ttl`**：交易自构建时刻起 `ttl` 内有效，到达 `ExpiresAt` 即过期。

构建时校验：阈值为 0、超过签名方人数、签名方为空、TTL ≤ 0 等均以
`invalid_argument` 拒绝。

审批判定顺序（结论稳定、互不掩盖）：

1. 交易必须处于 `pending` / `ready`，终态或在途状态审批 → `invalid_transition`；
2. 已过期 → 交易被 CAS 推进到终态 `expired`，返回 `expired`；
3. 审批人不在签名方集合 → `unauthorized_approver`（越权）；
4. 该签名方已审批过 → `duplicate_approval`（重复审批，不增加计数）；
5. 通过后追加审批；审批数达到阈值时 `pending -> ready`。

审批采用「读取 → 判定 → CAS」循环：不同签名方并发审批时，后写入者 CAS 失败后
**重读最新版本并重判**，只要自己的审批仍有效就会成功，不会被他人的并发审批误拒；
而同一签名方的并发重复审批，恰好一次成功，其余稳定返回 `duplicate_approval`。

## 3. 交易状态机

```
pending ──达到阈值──▶ ready ──┐
   │                   │      │
   └──────过期─────────┴──────┴─▶ expired（终态）
                          提交 │
                               ▼
                         submitted ──扣款成功──▶ executed（终态）
                               │
                               └──扣款失败──▶ failed ──显式 Retry──▶ ready
```

- 非法跃迁（如对 `executed` 再审批、对非 `failed` 交易 Retry、跳过阈值直接提交等）
  一律以 `invalid_transition` **拒绝**，绝不静默容忍。
- 执行失败不会让交易卡在 `submitted`：状态被显式置为 `failed` 并记录
  `FailureCode/FailureMessage`，必须调用 `Retry` 复位到 `ready` 后才能重新提交
  （失败恢复是显式动作，不会自动悄悄重试）。

## 4. nonce 与在途交易的并发管理

- **分配串行化**：`BuildTx` 在编排器互斥锁内按钱包分配连续 nonce（从 0 起，含在途
  预留）。相同 `(walletID, idempotencyKey)` 的重复/并发构建返回**同一笔交易**，
  不会分配第二个 nonce，也不会留下 nonce 空洞。
- **执行严格按序**：提交时 nonce 必须恰好等于「该钱包账本已执行最大 nonce + 1」
  （空钱包为 0）。前置交易尚在途时，后续交易会被**明确拒绝**为 `nonce_not_next`，
  而不是被静默排队或取舍。因此**结论与提交顺序无关**：乱序到达只会得到可区分的
  拒绝，按序补齐后所有交易仍依次执行，余额与 nonce 完全一致。
- **并发提交收敛**：同一 `txID` 的提交由每交易门（per-tx gate）串行化；账本再以
  `applied[txID]` 去重兜底。N 个并发提交的结果恒为 **恰好 1 次真实扣款 + N-1 次
  重放**。
- **互不覆盖**：交易状态用 `Version` 做乐观锁（CAS）；版本被并发改动时返回
  `nonce_conflict` 及最新数据，由调用方重读后决定，绝不后写覆盖先写。

## 5. 幂等与失败恢复语义

幂等的判定遵循「**账本优先于状态**」：

- 只要 `ledger.applied` 中存在该 `txID`，就认定交易已执行。此时无论交易状态停在
  `ready`/`submitted`/`executed`，重复提交都返回 `Replayed=true` 的执行回执，
  **不再扣款**，并在必要时把状态合法修复为 `executed`。
- `Debit` 在账本同一把锁内原子完成「余额扣减/收款方入账 + 已执行表 + nonce 游标」，
  随后原子落盘；落盘失败则内存变更一并回滚，不产生“扣了款但没记账”的半笔账。

进程启动（重载）时自动修复在途交易：

| 磁盘状态 | 账本记录 | 修复结果 |
| --- | --- | --- |
| `submitted` | 已扣款 | 修复为 `executed`（状态推进丢失，账没丢） |
| `submitted` | 无记录 | 修复为 `failed`（扣款未发生），需显式 `Retry` |

由此保证：重复请求不重复扣款、状态不丢失、不倒退；终态不可回退，非法跃迁被拒绝。

## 6. 错误分类

所有可预期错误均为 `*WalletError`，用稳定的 `ErrorCode` 区分；可用
`multisig.ErrorCodeOf(err)` 提取：

| Code | 触发场景 |
| --- | --- |
| `invalid_argument` | 参数非法：空钱包/幂等键/收款方、金额 ≤ 0、策略阈值或 TTL 非法 |
| `not_found` | 交易不存在、钱包策略未注册 |
| `unauthorized_approver` | 审批人不在签名方集合（审批越权） |
| `duplicate_approval` | 同一签名方对同一交易重复审批 |
| `policy_not_satisfied` | 提交时审批数未达阈值 |
| `expired` | 交易已超过 TTL（审批或提交时判定） |
| `invalid_transition` | 状态机不允许的跃迁（终态操作、未 Retry 直接提交 failed 等） |
| `nonce_conflict` | 状态 CAS 版本冲突、txID/幂等键冲突、对已扣款交易重复扣款 |
| `nonce_not_next` | 提交的 nonce 不是钱包当前可执行的最小 nonce（在途阻塞/乱序） |
| `insufficient_balance` | 钱包余额不足；交易进入 `failed`，充值后可 `Retry` |
| `internal` | 本地文件读写/序列化等内部错误（错误信息附带底层 cause） |

## 7. 测试覆盖与本地验证

测试全部在 `t.TempDir()` 中读写本地文件，**无外部依赖**，沿用仓库原生
`go test` 方式；日志打印关键输入与判定依据，便于复盘：

- `approval_policy_test.go`：越权/重复审批/阈值不足/过期/非法跃迁/余额不足与恢复；
- `concurrent_approval_test.go`：不同签名方并发全部成功、同一签名方并发恰好一次；
- `nonce_concurrency_test.go`：nonce 连续分配、乱序提交被明确拒绝、
  幂等键并发构建去重、同交易并发提交收敛、多交易乱序并发后按序一致完成；
- `recovery_test.go`：重载后重放不重复扣款、nonce 游标不倒退、
  两种「提交中途失败」的启动修复；
- `example_test.go`：端到端可运行示例。

```bash
go test -race -count=10 ./...   # 建议的本地确定性验证
```
