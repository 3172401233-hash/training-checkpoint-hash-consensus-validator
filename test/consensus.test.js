import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  evaluateCheckpointConsensus,
  manifestFingerprint,
  RANK_STATE,
  FAILURE_REASON,
} from '../src/consensus.js';

const SHARDS = ['shard-0', 'shard-1', 'shard-2'];

function manifest(version, digestPrefix = 'd') {
  return {
    version,
    shards: SHARDS.map((shardId, i) => ({ shardId, digest: `${digestPrefix}-${i}` })),
  };
}

function uploadsFor(mani, status = 'complete') {
  return mani.shards.map((s) => ({ shardId: s.shardId, digest: s.digest, status }));
}

function makeRank(rank, mani, { status = 'complete', retries = 0, uploads } = {}) {
  return { rank, manifest: mani, uploads: uploads ?? uploadsFor(mani, status), retries };
}

function baseInput(ranks, overrides = {}) {
  return {
    checkpointId: 'ckpt-2',
    parentCheckpoint: 'ckpt-1',
    expectedShards: SHARDS,
    quorum: 2,
    maxRetries: 3,
    commitPointer: { checkpointId: 'ckpt-1', version: 'v1' },
    ranks,
    ...overrides,
  };
}

test('多数清单一致且上传完成时可发布', () => {
  const mani = manifest('v2');
  const result = evaluateCheckpointConsensus(
    baseInput([makeRank(0, mani), makeRank(1, mani), makeRank(2, mani)]),
  );
  assert.equal(result.publishable, true);
  assert.equal(result.version, 'v2');
  assert.equal(result.rollbackPoint, null);
  assert.deepEqual(result.conflictingManifests, []);
  assert.equal(result.shards.length, 3);
  assert.ok(result.shards.every((s) => s.publishable && s.confirmations === 3));
  assert.ok(result.commitConditions.every((c) => c.met));
});

test('部分节点未完成上传时不可发布并返回回滚点', () => {
  const mani = manifest('v2');
  const ranks = [
    makeRank(0, mani),
    makeRank(1, mani, { status: 'pending' }),
    makeRank(2, mani, { status: 'pending' }),
  ];
  const result = evaluateCheckpointConsensus(baseInput(ranks));
  assert.equal(result.publishable, false);
  assert.deepEqual(result.rollbackPoint, { checkpointId: 'ckpt-1', version: 'v1' });
  assert.ok(result.reasons.includes(FAILURE_REASON.NO_QUORUM));
  const pending = result.rankReports.filter((r) => r.state === RANK_STATE.INCOMPLETE_UPLOAD);
  assert.equal(pending.length, 2);
});

test('清单分歧时少数派不能覆盖多数确认', () => {
  const majority = manifest('v2', 'a');
  const minority = manifest('v2-hotfix', 'b');
  const ranks = [
    makeRank(0, majority),
    makeRank(1, majority),
    makeRank(2, minority),
  ];
  const result = evaluateCheckpointConsensus(baseInput(ranks));
  assert.equal(result.publishable, true);
  assert.equal(result.version, 'v2');
  assert.deepEqual(result.conflictingManifests, [
    { version: 'v2-hotfix', ranks: [2], confirmations: 1 },
  ]);
  // 少数派分片摘要不同，但多数确认的分片仍可发布
  assert.ok(result.shards.every((s) => s.publishable && s.confirmations === 2));
});

test('清单三方分裂且无多数时不可发布', () => {
  const ranks = [
    makeRank(0, manifest('v2', 'a')),
    makeRank(1, manifest('v2', 'b')),
    makeRank(2, manifest('v2', 'c')),
  ];
  const result = evaluateCheckpointConsensus(baseInput(ranks));
  assert.equal(result.publishable, false);
  assert.ok(result.reasons.includes(FAILURE_REASON.NO_QUORUM));
  assert.equal(result.conflictingManifests.length, 3);
  assert.deepEqual(result.rollbackPoint, { checkpointId: 'ckpt-1', version: 'v1' });
});

test('清单摘要一致但部分分片确认不足 quorum 时不可发布', () => {
  const mani = manifest('v2');
  const ranks = [
    makeRank(0, mani),
    makeRank(1, mani),
    makeRank(2, mani, {
      uploads: mani.shards.map((s) => ({ ...s, digest: 'corrupt', status: 'complete' })),
    }),
  ];
  // rank2 摘要与清单不符被剔除，rank0/1 仍构成 quorum，可发布
  const ok = evaluateCheckpointConsensus(baseInput(ranks));
  assert.equal(ok.publishable, true);
  assert.equal(ok.rankReports.find((r) => r.rank === 2).state, RANK_STATE.DIGEST_MISMATCH);

  // 仅 rank0 上传完成，rank1 上传摘要不一致 -> 分片确认不足
  const ranks2 = [
    makeRank(0, mani),
    makeRank(1, mani, {
      uploads: mani.shards.map((s) => ({ ...s, digest: 'corrupt', status: 'complete' })),
    }),
    makeRank(2, mani, {
      uploads: mani.shards.map((s) => ({ ...s, digest: 'corrupt', status: 'complete' })),
    }),
  ];
  const bad = evaluateCheckpointConsensus(baseInput(ranks2));
  assert.equal(bad.publishable, false);
  assert.ok(bad.reasons.includes(FAILURE_REASON.NO_QUORUM));
});

test('提交竞争：提交指针已推进时拒绝发布并回滚到指针位置', () => {
  const mani = manifest('v2');
  const result = evaluateCheckpointConsensus(
    baseInput([makeRank(0, mani), makeRank(1, mani)], {
      commitPointer: { checkpointId: 'ckpt-2', version: 'v2-other' },
    }),
  );
  assert.equal(result.publishable, false);
  assert.ok(result.reasons.includes(FAILURE_REASON.COMMIT_RACE));
  assert.deepEqual(result.rollbackPoint, { checkpointId: 'ckpt-2', version: 'v2-other' });
  const pointer = result.commitConditions.find((c) => c.id === 'commit_pointer_at_parent');
  assert.equal(pointer.met, false);
});

test('清单未覆盖全部分片时不可发布', () => {
  const partial = { version: 'v2', shards: manifest('v2').shards.slice(0, 2) };
  const ranks = [
    { rank: 0, manifest: partial, uploads: uploadsFor(manifest('v2')).slice(0, 2) },
    { rank: 1, manifest: partial, uploads: uploadsFor(manifest('v2')).slice(0, 2) },
  ];
  const result = evaluateCheckpointConsensus(baseInput(ranks));
  assert.equal(result.publishable, false);
  assert.ok(
    result.rankReports.every((r) => r.state === RANK_STATE.COVERAGE_MISSING),
  );
});

test('重试次数超限的 rank 被剔除出共识', () => {
  const mani = manifest('v2');
  const ranks = [
    makeRank(0, mani),
    makeRank(1, mani),
    makeRank(2, mani, { retries: 5 }),
  ];
  const result = evaluateCheckpointConsensus(baseInput(ranks, { maxRetries: 3 }));
  assert.equal(result.publishable, true);
  assert.equal(
    result.rankReports.find((r) => r.rank === 2).state,
    RANK_STATE.RETRY_EXHAUSTED,
  );
});

test('manifestFingerprint 对分片顺序不敏感', () => {
  const a = manifest('v2');
  const b = { version: 'v2', shards: [...a.shards].reverse() };
  assert.equal(manifestFingerprint(a), manifestFingerprint(b));
});
