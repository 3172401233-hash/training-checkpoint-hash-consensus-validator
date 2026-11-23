/**
 * 训练检查点分片摘要共识验证器。
 *
 * 纯内存计算：不访问任何存储服务、网络或文件系统。
 * 输入各 rank 上报的清单、分片摘要、上传状态、父检查点、
 * quorum、重试次数与当前提交指针，判定是否存在可发布版本。
 */

export const RANK_STATE = Object.freeze({
  VALID: 'valid',
  INCOMPLETE_UPLOAD: 'incomplete_upload',
  DIGEST_MISMATCH: 'digest_mismatch',
  COVERAGE_MISSING: 'coverage_missing',
  RETRY_EXHAUSTED: 'retry_exhausted',
});

export const FAILURE_REASON = Object.freeze({
  COMMIT_RACE: 'commit_race',
  NO_QUORUM: 'no_quorum',
  SHARD_CONFIRMATION_LACKING: 'shard_confirmation_lacking',
});

/** 清单内容归一化后的稳定指纹（排序后序列化）。 */
export function manifestFingerprint(manifest) {
  const shards = [...(manifest?.shards ?? [])]
    .map((s) => ({ shardId: s.shardId, digest: s.digest }))
    .sort((a, b) => (a.shardId < b.shardId ? -1 : a.shardId > b.shardId ? 1 : 0));
  return JSON.stringify({ version: manifest?.version ?? null, shards });
}

function classifyRank(rank, expectedShardIds, maxRetries) {
  const problems = [];
  if ((rank.retries ?? 0) > maxRetries) {
    return { state: RANK_STATE.RETRY_EXHAUSTED, problems: ['retries_exceeded'] };
  }
  const manifest = rank.manifest;
  if (!manifest) {
    return { state: RANK_STATE.COVERAGE_MISSING, problems: ['manifest_missing'] };
  }
  const manifestShards = new Map(manifest.shards.map((s) => [s.shardId, s.digest]));
  const missing = expectedShardIds.filter((id) => !manifestShards.has(id));
  if (missing.length > 0) {
    problems.push(`missing_shards:${missing.join(',')}`);
  }
  const uploads = new Map((rank.uploads ?? []).map((u) => [u.shardId, u]));
  let uploadIncomplete = false;
  let digestMismatch = false;
  for (const shardId of expectedShardIds) {
    const upload = uploads.get(shardId);
    if (!upload || upload.status !== 'complete') {
      uploadIncomplete = true;
      continue;
    }
    const expected = manifestShards.get(shardId);
    if (expected !== undefined && upload.digest !== expected) {
      digestMismatch = true;
    }
  }
  if (digestMismatch) {
    problems.push('upload_digest_mismatch');
    return { state: RANK_STATE.DIGEST_MISMATCH, problems };
  }
  if (missing.length > 0) {
    return { state: RANK_STATE.COVERAGE_MISSING, problems };
  }
  if (uploadIncomplete) {
    return { state: RANK_STATE.INCOMPLETE_UPLOAD, problems: [...problems, 'upload_incomplete'] };
  }
  return { state: RANK_STATE.VALID, problems };
}

/**
 * 评估检查点共识。
 *
 * @param {object} input
 * @param {string} input.checkpointId 待提交的检查点 ID
 * @param {string} input.parentCheckpoint 父检查点 ID（期望的提交指针位置）
 * @param {string[]} input.expectedShards 必须被清单覆盖的全部分片 ID
 * @param {number} input.quorum 达成多数确认所需的最小 rank 数
 * @param {number} [input.maxRetries=3] rank 允许的最大重试次数
 * @param {{checkpointId: string, version?: string}} input.commitPointer 当前提交指针
 * @param {Array<{rank: string|number, manifest?: object, uploads?: Array, retries?: number}>} input.ranks
 */
export function evaluateCheckpointConsensus(input) {
  const {
    checkpointId,
    parentCheckpoint,
    expectedShards,
    quorum,
    maxRetries = 3,
    commitPointer,
    ranks = [],
  } = input;

  const rollbackPoint = {
    checkpointId: commitPointer?.checkpointId ?? parentCheckpoint,
    version: commitPointer?.version ?? null,
  };

  const commitConditions = [];
  const condition = (id, met, detail) => {
    commitConditions.push({ id, met, detail });
    return met;
  };

  // 提交竞争：提交指针已被其他提交推进，当前父检查点不再是链头。
  const pointerAtParent = commitPointer?.checkpointId === parentCheckpoint;
  condition(
    'commit_pointer_at_parent',
    pointerAtParent,
    pointerAtParent
      ? `提交指针位于父检查点 ${parentCheckpoint}`
      : `提交指针已推进到 ${commitPointer?.checkpointId}，期望 ${parentCheckpoint}`,
  );

  const rankReports = ranks.map((rank) => {
    const { state, problems } = classifyRank(rank, expectedShards, maxRetries);
    return {
      rank: rank.rank,
      state,
      problems,
      fingerprint: rank.manifest ? manifestFingerprint(rank.manifest) : null,
      version: rank.manifest?.version ?? null,
    };
  });

  const validReports = rankReports.filter((r) => r.state === RANK_STATE.VALID);
  const groups = new Map();
  for (const report of validReports) {
    if (!groups.has(report.fingerprint)) {
      groups.set(report.fingerprint, { fingerprint: report.fingerprint, version: report.version, ranks: [] });
    }
    groups.get(report.fingerprint).ranks.push(report.rank);
  }
  const sortedGroups = [...groups.values()].sort((a, b) => b.ranks.length - a.ranks.length);
  const winner = sortedGroups.find((g) => g.ranks.length >= quorum) ?? null;
  const conflictingManifests = sortedGroups
    .filter((g) => g !== winner)
    .map((g) => ({ version: g.version, ranks: g.ranks, confirmations: g.ranks.length }));

  condition(
    'manifest_quorum',
    winner !== null,
    winner
      ? `清单版本 ${winner.version} 获得 ${winner.ranks.length}/${ranks.length} 确认（quorum=${quorum}）`
      : `无任何清单达到 quorum=${quorum}（最大确认数 ${sortedGroups[0]?.ranks.length ?? 0}）`,
  );

  // 分片级确认：少数节点不能覆盖多数确认。
  // 对每个分片统计“上传完成且摘要与获胜清单一致”的 rank 数。
  const shardConfirmations = new Map(expectedShards.map((id) => [id, { confirmed: 0, digest: null }]));
  if (winner) {
    const winnerManifest = ranks.find((r) => r.manifest && manifestFingerprint(r.manifest) === winner.fingerprint).manifest;
    const expectedDigests = new Map(winnerManifest.shards.map((s) => [s.shardId, s.digest]));
    for (const rank of ranks) {
      const uploads = new Map((rank.uploads ?? []).map((u) => [u.shardId, u]));
      for (const shardId of expectedShards) {
        const upload = uploads.get(shardId);
        const digest = expectedDigests.get(shardId);
        if (upload && upload.status === 'complete' && digest !== undefined && upload.digest === digest) {
          const entry = shardConfirmations.get(shardId);
          entry.confirmed += 1;
          entry.digest = digest;
        }
      }
    }
  }

  const shards = expectedShards.map((shardId) => {
    const entry = shardConfirmations.get(shardId);
    return {
      shardId,
      digest: entry.digest,
      confirmations: entry.confirmed,
      publishable: winner !== null && entry.confirmed >= quorum,
    };
  });
  const allShardsConfirmed = winner !== null && shards.every((s) => s.publishable);
  condition(
    'shard_quorum',
    allShardsConfirmed,
    allShardsConfirmed
      ? '全部分片达到 quorum 确认'
      : `未确认分片: ${shards.filter((s) => !s.publishable).map((s) => s.shardId).join(',') || '无（无获胜清单）'}`,
  );

  const publishable = pointerAtParent && winner !== null && allShardsConfirmed;
  const reasons = [];
  if (!pointerAtParent) reasons.push(FAILURE_REASON.COMMIT_RACE);
  if (!winner) reasons.push(FAILURE_REASON.NO_QUORUM);
  if (winner && !allShardsConfirmed) reasons.push(FAILURE_REASON.SHARD_CONFIRMATION_LACKING);

  return {
    checkpointId,
    publishable,
    version: publishable ? winner.version : null,
    shards,
    conflictingManifests,
    commitConditions,
    rollbackPoint: publishable ? null : rollbackPoint,
    rankReports,
    reasons,
  };
}
