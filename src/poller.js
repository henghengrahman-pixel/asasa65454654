import crypto from 'node:crypto';
import { config } from './config.js';
import { extractChatEvents } from './livechat.js';
import { normalizeText, detectIntent } from './normalizer.js';
import { processCustomerMessage, processGreetingTrigger } from './engine.js';
import * as db from './db.js';
import { isGreetingTriggerMessage } from './greeting.js';

let running=false, started=false, timer=null, lastTick=null, lastError=null, lastResult=null, paused=false;
let deepTimer=null, deepSyncRunning=false, deepLastRun=null, deepLastError=null, inventorySize=0, inventoryComplete=false;
const summaryFingerprints = new Map();
export function forgetChat(chatId){ summaryFingerprints.delete(String(chatId||'')); }
export function pollerStatus(){ return {running:started,inFlight:running,paused,lastTick,lastError,lastResult,mode:config.lcSyncMode,pollMs:config.lcPollMs,trackedChats:summaryFingerprints.size,deepSyncRunning,deepLastRun,deepLastError,inventorySize,inventoryComplete}; }

function senderType(ev, chat){
  const t=String(ev.authorType||'').toLowerCase();
  if (t.includes('customer')) return 'customer';
  if (t.includes('agent')) return 'agent';
  const u=(chat?.users||[]).find(x=>String(x.id||'')===String(ev.authorId||''));
  const ut=String(u?.type||'').toLowerCase();
  if (ut.includes('customer')) return 'customer';
  if (ut.includes('agent')) return 'agent';
  return 'unknown';
}

function summaryFingerprint(summary){ return crypto.createHash('sha1').update(JSON.stringify(summary||{})).digest('hex'); }
function ageSeconds(iso){ const t=Date.parse(iso||''); return Number.isFinite(t) ? Math.max(0,(Date.now()-t)/1000) : Infinity; }
function isWelcomeTriggerEvent(ev){ return Boolean(ev && isGreetingTriggerMessage(ev.text)); }
function greetingTriggerAgeLimit(){ return Math.max(Number(config.greetingTriggerMaxAgeSeconds||0),600); }

async function ingestAgentEvent(chatId, ev, chat, livechat, {allowTakeover=true,allowGreetingTrigger=true}={}) {
  const ours=await db.outboundLooksLikeOurs(chatId,ev.eventId,ev.text);
  const autoGreetingTrigger=!ours && isGreetingTriggerMessage(ev.text);
  const senderType=ours?'ai':autoGreetingTrigger?'system':'agent';
  const inserted=await db.insertMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,senderType,authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:autoGreetingTrigger?'GREETING_TRIGGER':detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]});
  if (inserted && autoGreetingTrigger && allowGreetingTrigger && ageSeconds(ev.createdAt) <= greetingTriggerAgeLimit()) {
    await processGreetingTrigger({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat});
  }
  if (inserted && !ours && !autoGreetingTrigger) {
    await db.captureHumanReplyLearning({chatId,eventId:ev.eventId,responseText:ev.text}).catch(()=>{});
  }
  if (inserted && allowTakeover && !ours && !autoGreetingTrigger && ageSeconds(ev.createdAt) <= config.humanTakeoverMinutes*60) {
    await db.setHumanTakeover(chatId,'agent_reply_livechat');
  }
  return inserted;
}

async function ensureFreshWelcomeGreeting(chatId, events, livechat){
  const fresh=[...(events||[])].reverse().find(ev=>isWelcomeTriggerEvent(ev) && ageSeconds(ev.createdAt)<=greetingTriggerAgeLimit());
  if(!fresh) return null;
  await db.insertMessage({chatId,eventId:fresh.eventId,threadId:fresh.threadId,senderType:'system',authorId:fresh.authorId||'system',text:fresh.text,normalizedText:normalizeText(fresh.text),intent:'GREETING_TRIGGER',createdAt:fresh.createdAt,attachments:fresh.attachments||[]}).catch(()=>{});
  return processGreetingTrigger({chatId,eventId:fresh.eventId,threadId:fresh.threadId,text:fresh.text,createdAt:fresh.createdAt,livechat}).catch(async e=>{
    await db.logError('poller','WELCOME_GREETING_RETRY_FAILED',e.message,{chatId,eventId:fresh.eventId}).catch(()=>{}); return {error:e.message};
  });
}

async function bootstrapChat(chatId, chat, events, livechat) {
  let inserted=0, processed=0;
  await ensureFreshWelcomeGreeting(chatId,events,livechat);
  const latest=events.at(-1);
  for (const ev of events.slice(0,-1)) {
    const type=senderType(ev,chat);
    if (isWelcomeTriggerEvent(ev)) {
      if (await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true})) inserted++;
    } else if (type==='customer') {
      if (await db.insertMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]})) inserted++;
    } else if (type==='agent') {
      if (await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true})) inserted++;
    }
  }
  if (latest) {
    const type=senderType(latest,chat);
    if (isWelcomeTriggerEvent(latest)) {
      if (await ingestAgentEvent(chatId,latest,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true})) inserted++;
    } else if (type==='customer' && ageSeconds(latest.createdAt) <= config.bootstrapReplyMaxAgeSeconds) {
      const r=await processCustomerMessage({chatId,eventId:latest.eventId,threadId:latest.threadId,text:latest.text,createdAt:latest.createdAt,livechat,attachments:latest.attachments||[]});
      if (!r?.skipped) processed++;
      if (r?.skipped!=='duplicate') inserted++;
    } else if (type==='customer') {
      if (await db.insertMessage({chatId,eventId:latest.eventId,threadId:latest.threadId,senderType:'customer',authorId:latest.authorId,text:latest.text,normalizedText:normalizeText(latest.text),intent:detectIntent(latest.text),createdAt:latest.createdAt,attachments:latest.attachments||[]})) inserted++;
    } else if (type==='agent') {
      if (await ingestAgentEvent(chatId,latest,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true})) inserted++;
    }
  }
  await db.markBootstrapped(chatId);
  return {inserted,processed};
}

async function processChatSnapshot(livechat,chat,summary=null,rank=null,{force=false}={}){
  const chatId=String(chat?.id||summary?.id||'').trim(); if(!chatId) return {skipped:'missing_chat_id'};
  const source=summary||chat;
  const state={...livechat.chatState(source),rank};
  if(state.active===false){ await db.markConversationEnded(chatId); summaryFingerprints.delete(chatId); return {closed:true}; }
  await db.upsertConversation(source,{visible:true,state});
  await db.updateTypingFromSummary(chatId,source).catch(()=>{});
  const fp=summaryFingerprint(source);
  const oldFp=summaryFingerprints.get(chatId);
  const dbState=await db.getConversationState(chatId);
  if(!force && dbState?.bootstrapped_at && Number(dbState?.message_count||0)>0 && oldFp===fp) return {unchanged:1};

  let detail=chat;
  if(!detail || detail===summary || extractChatEvents(detail).length===0){ detail=await livechat.getChat(chatId,source); }
  if(!detail?.id) return {skipped:'empty_detail'};
  const detailState={...livechat.chatState(detail),rank};
  if(detailState.active===false){ await db.markConversationEnded(chatId); summaryFingerprints.delete(chatId); return {closed:true}; }
  await db.upsertConversation(detail,{visible:true,state:{...state,...detailState}});
  const events=extractChatEvents(detail);
  if(!events.length){ await db.clearBootstrapped(chatId); summaryFingerprints.delete(chatId); await db.logError('poller','EMPTY_CHAT_DETAIL','LiveChat get_chat returned no readable message events',{chatId,diagnostics:livechat.chatDiagnostics(detail)}); return {empty:1}; }

  const before=await db.getConversationState(chatId);
  await ensureFreshWelcomeGreeting(chatId,events,livechat);
  if(!before?.bootstrapped_at || Number(before?.message_count||0)===0){
    const b=await bootstrapChat(chatId,detail,events,livechat); summaryFingerprints.set(chatId,fp); return {...b,bootstrapped:1,fetched:1};
  }

  let newMessages=0,processed=0,deferredMs=0;
  const unseen=[];
  for(const ev of events){
    const exists=await db.messageExists(chatId,ev.eventId);
    if(!exists){ unseen.push(ev); continue; }
    // If a reply attempt failed after the customer message was already persisted, the
    // durable processing ledger marks it retryable. Do not let messageExists() hide it.
    if(senderType(ev,detail)==='customer' && await db.customerEventNeedsRetry(chatId,ev.eventId,{staleSeconds:config.lcIngressStaleSeconds})) unseen.push(ev);
  }
  const newest=unseen.at(-1)||null;
  for (const ev of unseen) {
    const type=senderType(ev,detail); const isNewest=newest && ev.eventId===newest.eventId;
    if (isWelcomeTriggerEvent(ev)) {
      if (await ingestAgentEvent(chatId,ev,detail,livechat,{allowTakeover:false,allowGreetingTrigger:true})) newMessages++;
    } else if (type==='customer' && isNewest) {
      if (ageSeconds(ev.createdAt)*1000 < config.memberDebounceMs) { deferredMs=Math.max(deferredMs,Math.ceil(config.memberDebounceMs-ageSeconds(ev.createdAt)*1000)); summaryFingerprints.delete(chatId); continue; }
      const result=await processCustomerMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat,attachments:ev.attachments||[]});
      if(!result?.skipped) processed++; if(result?.skipped!=='duplicate') newMessages++;
    } else if (type==='customer') {
      if(await db.insertMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]})) newMessages++;
    } else if (type==='agent') {
      if(await ingestAgentEvent(chatId,ev,detail,livechat,{allowTakeover:true,allowGreetingTrigger:true})) newMessages++;
    }
  }
  await ensureFreshWelcomeGreeting(chatId,events,livechat);
  if(!newest || await db.messageExists(chatId,newest.eventId)) summaryFingerprints.set(chatId,fp); else summaryFingerprints.delete(chatId);
  return {newMessages,processed,fetched:1,deferredMs};
}

export async function syncChatById(livechat,chatId,summary={}){
  const id=String(chatId||'').trim(); if(!id) throw new Error('LIVECHAT_CHAT_ID_REQUIRED');
  const seed=summary&&summary.id?summary:{id};
  const chat=await livechat.getChat(id,seed);
  return processChatSnapshot(livechat,chat,summary&&summary.id?summary:null,null,{force:true});
}

async function runConcurrent(items,limit,fn){
  let cursor=0; const n=Math.max(1,Math.min(Number(limit)||1,32));
  const workers=Array.from({length:Math.min(n,items.length||1)},async()=>{while(true){const i=cursor++; if(i>=items.length) break; await fn(items[i],i);}});
  await Promise.all(workers);
}

async function runDeepSync(livechat){
  if(deepSyncRunning) return {skipped:'already_running'};
  deepSyncRunning=true; deepLastError=null; const started=Date.now(); inventoryComplete=false;
  try{
    const all=await livechat.listAllActiveChats(); const chats=livechat.filterInbox(all.items||[]); const seenChatIds=[];
    inventorySize=chats.length;
    for (const [rank, summary] of chats.entries()) {
      if(!summary?.id) continue; const chatId=String(summary.id); seenChatIds.push(chatId);
      const state={...livechat.chatState(summary),rank}; await db.upsertConversation(summary,{visible:true,state});
      const fp=summaryFingerprint(summary);
      await db.enqueueLiveChatIngressJob({dedupeKey:`poll:${chatId}:${fp}`,jobType:'SYNC_CHAT',chatId,payload:{summary}}).catch(()=>{});
    }
    // A full active-chat inventory is authoritative. Only here may absent rows be hidden/closed.
    const previous=await db.listVisibleConversationIds(); const active=new Set(seenChatIds);
    for(const id of previous) if(!active.has(id)){await db.markConversationEnded(id).catch(()=>{});summaryFingerprints.delete(String(id));}
    // Keep the in-memory change detector bounded to the current active inventory. It must
    // never grow with historical/closed chats over a long-running Railway deployment.
    for(const id of summaryFingerprints.keys()) if(!active.has(String(id))) summaryFingerprints.delete(id);
    await db.reconcileInboxVisibility(seenChatIds,25);
    inventoryComplete=true; deepLastRun=new Date().toISOString();
    await db.setIntegrationHealth('livechat_deep_sync',{status:'OK',latencyMs:Date.now()-started,meta:{activeChats:chats.length,pages:all.pages,foundChats:all.foundChats}}).catch(()=>{});
    return {ok:true,activeChats:chats.length,pages:all.pages};
  }catch(e){deepLastError=e.message;await db.setIntegrationHealth('livechat_deep_sync',{status:'ERROR',latencyMs:Date.now()-started,error:e.message}).catch(()=>{});throw e;}
  finally{deepSyncRunning=false;}
}

export async function syncOnce(livechat,{manual=false}={}){
  if (!manual) { const enabled=Boolean(await db.getSetting('system_enabled',true)); if (!enabled) { paused=true; lastResult={ok:true,paused:true,skipped:'system_off'}; return lastResult; } }
  paused=false; if(running) return {skipped:'already_running'}; running=true; lastError=null; const syncStarted=Date.now();
  try{
    const data=await livechat.listChats();
    const rawChats=data?._normalizedChats || data?.chats_summary || data?.chats || [];
    const chats=livechat.filterInbox(rawChats);
    let queued=0,unchanged=0,enqueueErrors=0;
    // The 1-second hot path performs discovery only. It never waits for get_chat, OpenAI,
    // Telegram, or a member reply. Expensive work is persisted into PostgreSQL and consumed
    // by the durable ingress worker, keeping discovery responsive during traffic bursts.
    for (const [rank,summary] of chats.entries()){
      if(!summary?.id) continue;
      const chatId=String(summary.id);
      try{
        const state={...livechat.chatState(summary),rank};
        await db.upsertConversation(summary,{visible:true,state});
        const fp=summaryFingerprint(summary);
        const oldFp=summaryFingerprints.get(chatId);
        if(oldFp===fp){unchanged++;continue;}
        const job=await db.enqueueLiveChatIngressJob({dedupeKey:`poll:${chatId}:${fp}`,jobType:'SYNC_CHAT',chatId,payload:{summary},priority:60});
        if(job)queued++;else unchanged++;
        summaryFingerprints.set(chatId,fp);
      }catch(e){enqueueErrors++;summaryFingerprints.delete(chatId);await db.logError('poller','DISCOVERY_ENQUEUE_FAILED',e.message,{chatId}).catch(()=>{});}
    }
    lastTick=new Date().toISOString();
    lastResult={ok:true,listSource:data?._listSource||'unknown',rawChats:rawChats.length,chats:chats.length,queued,unchanged,enqueueErrors,processed:0,newMessages:0,inventorySize,inventoryComplete,deepSyncRunning,concurrency:config.lcSyncConcurrency,durationMs:Date.now()-syncStarted};
    await db.setIntegrationHealth('livechat_poll',{status:'OK',latencyMs:Date.now()-syncStarted,meta:{chats:chats.length,queued,unchanged,enqueueErrors}}).catch(()=>{}); return lastResult;
  }catch(e){lastError=e.message;await db.setIntegrationHealth('livechat_poll',{status:'ERROR',latencyMs:Date.now()-syncStarted,error:e.message}).catch(()=>{});await db.logError('poller','SYNC_FAILED',e.message);throw e;}
  finally{running=false;}
}

export function startPoller(livechat){
  if(config.lcSyncMode==='off'){started=false;return;}
  if(timer||deepTimer){started=true;return;}
  started=true;
  const run=async()=>{try{await syncOnce(livechat);}catch{}finally{timer=setTimeout(run,config.lcPollMs);}};
  const deep=async()=>{try{await runDeepSync(livechat);}catch{}finally{deepTimer=setTimeout(deep,config.lcDeepSyncMs);}};
  timer=setTimeout(run,1200); deepTimer=setTimeout(deep,1800);
}
export async function stopPoller({waitMs=5000}={}){ started=false;if(timer)clearTimeout(timer);if(deepTimer)clearTimeout(deepTimer);timer=null;deepTimer=null;const until=Date.now()+waitMs;while((running||deepSyncRunning)&&Date.now()<until)await new Promise(r=>setTimeout(r,50));return !running&&!deepSyncRunning; }
