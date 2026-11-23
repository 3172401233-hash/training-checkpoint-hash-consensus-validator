# training-checkpoint-hash-consensus-validator

验证检查点分片摘要、提交清单和节点共识，判定是否存在可发布的检查点版本。

## 背景

分布式训练中，多个 rank 各自上传检查点分片和清单。可能出现：

- 部分分片摘要一致，但清单版本互相冲突；
- 另一些节点尚未完成上传。

过早提交会让恢复读到混合版本。本模块在提交前做共识判定：
清单必须覆盖全部分片并通过摘要验证，少数节点不能覆盖多数确认。

## 输入

`evaluateCheckpointConsensus(input)`（见 `src/consensus.js`）：

| 字段 | 说明 |
| --- | --- |
| `checkpointId` | 待提交的检查点 ID |
| `parentCheckpoint` | 父检查点 ID（期望的提交指针位置） |
| `expectedShards` | 必须被清单覆盖的全部分片 ID |
| `quorum` | 达成多数确认所需的最小 rank 数 |
| `maxRetries` | rank 允许的最大重试次数（默认 3，超限剔除） |
| `commitPointer` | 当前提交指针 `{ checkpointId, version }` |
| `ranks` | 各 rank 上报的 `manifest`、`uploads`（摘要 + 状态）、`retries` |

## 输出

- `publishable` / `version`：是否存在可发布版本及其清单版本；
- `shards`：每个分片的摘要、确认数与是否可发布（需达到 quorum）；
- `conflictingManifests`：未获胜的冲突清单及其支持者；
- `commitConditions`：提交条件逐项判定（指针在父检查点、清单 quorum、分片 quorum）；
- `rollbackPoint`：不可发布时的回滚点（当前提交指针，即最后已知一致版本）；
- `rankReports` / `reasons`：各 rank 状态与失败原因（`commit_race`、`no_quorum`、`shard_confirmation_lacking`）。

## 不访问存储服务

本模块为**纯内存计算**：不访问任何对象存储、文件系统或网络。
分片摘要与上传状态由调用方（上传流水线/控制面）采集后传入，
模块只做一致性验证与共识判定，因此可安全地在提交路径上同步调用，
也便于在测试中构造部分上传、清单分歧、提交竞争等场景。

## 测试

```sh
node --test
```

要求 Node.js >= 24.16.0。测试覆盖：多数一致发布、部分上传、
清单分歧（少数派不覆盖多数）、三方分裂无 quorum、分片确认不足、
提交竞争（指针已推进）、清单覆盖缺失、重试超限剔除。
