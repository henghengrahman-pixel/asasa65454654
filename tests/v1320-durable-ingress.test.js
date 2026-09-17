import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {LiveChatClient} from '../src/livechat.js';
import {normalizeLiveChatWebhook} from '../src/livechat-webhook.js';

test('v1.32 list_chats uses documented active filter and page_id-only continuation',async()=>{
  const calls=[];
  const lc=new LiveChatClient({base:'http://local',accountId:'a',pat:'p'});
  lc.call=async(action,body)=>{
    calls.push({action,body});
    if(calls.length===1) return {found_chats:2,next_page_id:'p2',chats_summary:[{id:'c1',last_thread_summary:{active:true}}]};
    return {chats_summary:[{id:'c2',last_thread_summary:{active:true}}]};
  };
  const out=await lc.listAllActiveChats();
  assert.equal(out.items.length,2);
  assert.deepEqual(calls[0].body,{filters:{active:true,include_chats_without_threads:true},sort_order:'desc',limit:100});
  assert.deepEqual(calls[1].body,{page_id:'p2'});
});

test('v1.32 full pagination has no application page cap at 5000 active chats',async()=>{
  let page=0;
  const lc=new LiveChatClient({base:'http://local',accountId:'a',pat:'p'});
  lc.call=async()=>{
    const start=page++*100;
    const chats_summary=Array.from({length:100},(_,i)=>({id:`c${start+i}`,last_thread_summary:{active:true}}));
    return {chats_summary,...(page<50?{next_page_id:`page-${page}`}:{})};
  };
  const out=await lc.listAllActiveChats();
  assert.equal(out.items.length,5000);
  assert.equal(out.pages,50);
});

test('v1.32 webhook normalizer recognizes incoming events and deactivation',()=>{
  const incoming=normalizeLiveChatWebhook({action:'incoming_event',payload:{chat_id:'c1',thread_id:'t1',event:{id:'e1',type:'message',text:'halo'}}});
  assert.equal(incoming.chatId,'c1'); assert.equal(incoming.threadId,'t1'); assert.equal(incoming.eventId,'e1'); assert.equal(incoming.deactivated,false);
  const closed=normalizeLiveChatWebhook({action:'chat_deactivated',payload:{chat_id:'c1',thread_id:'t1'}});
  assert.equal(closed.deactivated,true);
});

test('v1.32 durable queue uses PostgreSQL SKIP LOCKED and stale reclaim',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  assert.match(db,/CREATE TABLE IF NOT EXISTS livechat_ingress_jobs/);
  assert.match(db,/FOR UPDATE SKIP LOCKED/);
  assert.match(db,/status='PROCESSING' AND locked_at < now\(\)/);
  assert.match(db,/UNIQUE/);
});

test('v1.32 closed chat is guarded before outbound send',()=>{
  const engine=fs.readFileSync(new URL('../src/engine.js',import.meta.url),'utf8');
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  assert.match(engine,/conversationCanReply\(chatId\)/);
  assert.match(engine,/CHAT_CLOSED_NO_REPLY/);
  assert.match(db,/status,lc_active/);
});

test('v1.32 customer-event processing ledger retries a persisted event after reply failure',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  const engine=fs.readFileSync(new URL('../src/engine.js',import.meta.url),'utf8');
  const poller=fs.readFileSync(new URL('../src/poller.js',import.meta.url),'utf8');
  assert.match(db,/CREATE TABLE IF NOT EXISTS customer_event_processing/);
  assert.match(db,/claimCustomerEventProcessing/);
  assert.match(db,/finishCustomerEventProcessing/);
  assert.match(db,/customerEventNeedsRetry/);
  assert.match(engine,/claimCustomerEventProcessing\(chatId,eventId/);
  assert.match(engine,/finishCustomerEventProcessing\(chatId,eventId,\{ok:false/);
  assert.match(poller,/customerEventNeedsRetry\(chatId,ev\.eventId/);
});

test('v1.32 ingress serializes jobs per chat and deactivation closes the send gate immediately',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  const ingress=fs.readFileSync(new URL('../src/livechat-ingress.js',import.meta.url),'utf8');
  assert.match(db,/row_number\(\) OVER \(PARTITION BY COALESCE\(j\.chat_id/);
  assert.match(db,/NOT EXISTS \(\s*SELECT 1 FROM livechat_ingress_jobs x WHERE x\.chat_id=j\.chat_id AND x\.status='PROCESSING'/s);
  assert.match(ingress,/markConversationInactive\(n\.chatId,/);
  assert.match(ingress,/cancelLiveChatIngressJobs\(n\.chatId,job\.id\)/);
  assert.doesNotMatch(ingress,/job\.event_id && await db\.messageExists/);
});

test('v1.32 send_event payloads match Agent Chat Web API v3.6 exactly',async()=>{
  const calls=[];
  const lc=new LiveChatClient({base:'http://local',accountId:'a',pat:'p'});
  lc.call=async(action,body,options)=>{calls.push({action,body,options});return {event_id:'out1'};};
  await lc.sendMessage('c1','halo');
  await lc.sendFile('c1',{url:'https://cdn.example/image.png',name:'ignored.png',contentType:'image/png',size:123});
  assert.deepEqual(calls[0],{action:'send_event',body:{chat_id:'c1',event:{type:'message',text:'halo',visibility:'all'}},options:{retries:0}});
  assert.deepEqual(calls[1],{action:'send_event',body:{chat_id:'c1',event:{type:'file',url:'https://cdn.example/image.png',visibility:'all'}},options:{retries:0}});
  const src=fs.readFileSync(new URL('../src/livechat.js',import.meta.url),'utf8');
  assert.doesNotMatch(src,/attach_to_last_thread/);
  assert.doesNotMatch(src,/form\.append\('chat_id'/);
});

test('v1.32 cancelled close-race jobs cannot be resurrected by a finishing worker',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  assert.match(db,/finishLiveChatIngressJob[\s\S]*WHERE id=\$1 AND status='PROCESSING'/);
  assert.match(db,/retryLiveChatIngressJob[\s\S]*WHERE id=\$1 AND status='PROCESSING'/);
});

test('v1.32 one-second poller is discovery-only and never waits on AI processing',()=>{
  const src=fs.readFileSync(new URL('../src/poller.js',import.meta.url),'utf8');
  const start=src.indexOf('export async function syncOnce');
  const end=src.indexOf('export function startPoller',start);
  const hot=src.slice(start,end);
  assert.match(hot,/enqueueLiveChatIngressJob/);
  assert.match(hot,/DISCOVERY_ENQUEUE_FAILED/);
  assert.doesNotMatch(hot,/processChatSnapshot\(/);
  assert.doesNotMatch(hot,/processCustomerMessage\(/);
  assert.doesNotMatch(hot,/livechat\.getChat\(/);
});

test('v1.32 stale deactivation from an older thread cannot close a newer reused chat session',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  const ingress=fs.readFileSync(new URL('../src/livechat-ingress.js',import.meta.url),'utf8');
  assert.match(db,/current && current!==tid/);
  assert.match(db,/STALE_THREAD_DEACTIVATION/);
  assert.match(db,/current_thread_id IS NULL OR current_thread_id=\$2/);
  assert.match(ingress,/threadId:job\.thread_id\|\|null/);
});
