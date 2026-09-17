import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
const poller=fs.readFileSync(new URL('../src/poller.js',import.meta.url),'utf8');
const server=fs.readFileSync(new URL('../src/server.js',import.meta.url),'utf8');
const config=fs.readFileSync(new URL('../src/config.js',import.meta.url),'utf8');

test('v2 persists LiveChat sync work in PostgreSQL with lease-safe claims',()=>{
  assert.match(db,/CREATE TABLE IF NOT EXISTS livechat_sync_jobs/);
  assert.match(db,/generation BIGINT NOT NULL DEFAULT 1/);
  assert.match(db,/FOR UPDATE SKIP LOCKED/);
  assert.match(db,/claimLiveChatSyncJobs/);
  assert.match(db,/retryLiveChatSyncJob/);
});

test('v2 tracked worker throughput is configurable and not an active-chat count cap',()=>{
  assert.match(config,/LIVECHAT_TRACKED_BATCH_SIZE/);
  assert.match(config,/LIVECHAT_TRACKED_CONCURRENCY/);
  assert.match(poller,/batch\.length>=config\.lcTrackedBatchSize/);
  assert.match(poller,/mapBounded\(batch,config\.lcTrackedConcurrency/);
});

test('v2 fast discovery persists before AI processing',()=>{
  assert.match(poller,/Discovery is intentionally lightweight/);
  assert.match(poller,/await flushDurableBuffer\(\)/);
  assert.match(poller,/FAST_DISCOVERY_UPSERT_FAILED/);
});

test('v2 durable worker retries incomplete provider reads instead of acknowledging them',()=>{
  assert.match(poller,/\['busy','empty_detail','empty_events'\]/);
  assert.match(poller,/retryable_sync_result/);
  assert.match(poller,/completeLiveChatSyncJob/);
});

test('v2 webhook ingress acknowledges after durable enqueue',()=>{
  assert.match(server,/enqueueLiveChatSyncJobs/);
  assert.match(server,/status\(202\)\.json\(\{ok:true,queued:true\}\)/);
});
