import crypto from 'node:crypto';
import { config } from './config.js';
import { extractChatEvents } from './livechat.js';
import { normalizeText, detectIntent } from './normalizer.js';
import { processCustomerMessage, processGreetingTrigger } from './engine.js';
import * as db from './db.js';
import { isGreetingTriggerMessage } from './greeting.js';

let loopStarted=false, inFlight=false, timer=null, deepTimer=null, trackedTimer=null, lastTick=null, lastSuccess=null, lastError=null, lastResult=null, paused=false;
const summaryFingerprints = new Map();
const trackedChatIds = new Set();
const trackedSummaries = new Map();
const trackedQueue = new Map();
const durableBuffer = new Map();
let durableFlushRunning=false;
const processingChatIds = new Set();
let trackedWorkerRunning=false;
const deepState={status:'IDLE',running:false,generation:0,completedGeneration:0,lastSuccess:null,lastError:null,pagesFetched:0,inventorySize:0,inventoryComplete:false,lastDurationMs:0};
export function pollerStatus(){ return {running:inFlight,started:loopStarted,inFlight,paused,lastTick,lastSuccess,lastError,lastResult,mode:config.lcSyncMode,pollMs:config.lcPollMs,trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size,trackedWorkerRunning,durableBufferDepth:durableBuffer.size,deepSyncRunning:deepState.running,deepStatus:deepState.status,deepGeneration:deepState.generation,deepCompletedGeneration:deepState.completedGeneration,deepLastSuccess:deepState.lastSuccess,deepLastError:deepState.lastError,inventoryComplete:deepState.inventoryComplete,pagesFetched:deepState.pagesFetched,inventorySize:deepState.inventorySize}; }

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

function summaryFingerprint(summary){ const value=(summary&&typeof summary==='object')?{...summary}:{}; delete value.rank; return crypto.createHash('sha1').update(JSON.stringify(value)).digest('hex'); }
function ageSeconds(iso){ const t=Date.parse(iso||''); return Number.isFinite(t) ? Math.max(0,(Date.now()-t)/1000) : Infinity; }
function isWelcomeTriggerEvent(ev){ return Boolean(ev && isGreetingTriggerMessage(ev.text)); }
function greetingTriggerAgeLimit(){ return Math.max(Number(config.greetingTriggerMaxAgeSeconds||0),600); }


async function ingestAgentEvent(chatId, ev, chat, livechat, {allowTakeover=true,allowGreetingTrigger=true}={}) {
  const ours=await db.outboundLooksLikeOurs(chatId,ev.eventId,ev.text);
  const autoGreetingTrigger=!ours && isGreetingTriggerMessage(ev.text);
  const senderType=ours?'ai':autoGreetingTrigger?'system':'agent';
  const inserted=await db.insertMessage({chatId,eventId:ev.eventId,senderType,authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:autoGreetingTrigger?'GREETING_TRIGGER':detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]});
  if (inserted && autoGreetingTrigger && allowGreetingTrigger && ageSeconds(ev.createdAt) <= greetingTriggerAgeLimit()) {
    await processGreetingTrigger({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat});
  }
  if (inserted && !ours && !autoGreetingTrigger) {
    // Human CS replies become learning candidates. They are NOT auto-approved;
    // admin reviews them in "Belajar dari CS" so one bad answer cannot poison the bot.
    await db.captureHumanReplyLearning({chatId,eventId:ev.eventId,responseText:ev.text}).catch(()=>{});
  }
  if (inserted && allowTakeover && !ours && !autoGreetingTrigger && ageSeconds(ev.createdAt) <= config.humanTakeoverMinutes*60) {
    await db.setHumanTakeover(chatId,'agent_reply_livechat');
  }
  return inserted;
}

async function ensureFreshWelcomeGreeting(chatId, events, livechat){
  const fresh=[...(events||[])].reverse().find(ev=>
    isWelcomeTriggerEvent(ev) && ageSeconds(ev.createdAt)<=greetingTriggerAgeLimit()
  );
  if(!fresh) return null;

  // Session-boundary preflight. Persist the banner BEFORE any customer event is
  // classified so current-session history can never leak old DP/WD/proof state.
  await db.insertMessage({
    chatId,eventId:fresh.eventId,senderType:'system',authorId:fresh.authorId||'system',
    text:fresh.text,normalizedText:normalizeText(fresh.text),intent:'GREETING_TRIGGER',
    createdAt:fresh.createdAt,attachments:fresh.attachments||[]
  }).catch(()=>{});

  // Idempotent by banner event id. This may be called every sync; only a genuinely
  // new System banner can claim and send a greeting.
  return processGreetingTrigger({
    chatId,
    eventId:fresh.eventId,
    threadId:fresh.threadId,
    text:fresh.text,
    createdAt:fresh.createdAt,
    livechat
  }).catch(async e=>{
    await db.logError('poller','WELCOME_GREETING_RETRY_FAILED',e.message,{chatId,eventId:fresh.eventId}).catch(()=>{});
    return {error:e.message};
  });
}

async function bootstrapChat(chatId, chat, events, livechat) {
  let inserted=0, processed=0;
  // IMPORTANT: establish/reset the new session before reading any member intent.
  await ensureFreshWelcomeGreeting(chatId,events,livechat);
  const latest=events.at(-1);
  for (const ev of events.slice(0,-1)) {
    const type=senderType(ev,chat);
    // System welcome text has absolute priority over author classification. LiveChat
    // can occasionally expose the banner with an unexpected author type.
    if (isWelcomeTriggerEvent(ev)) {
      if (await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true})) inserted++;
    } else if (type==='customer') {
      if (await db.insertMessage({chatId,eventId:ev.eventId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]})) inserted++;
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
      if (await db.insertMessage({chatId,eventId:latest.eventId,senderType:'customer',authorId:latest.authorId,text:latest.text,normalizedText:normalizeText(latest.text),intent:detectIntent(latest.text),createdAt:latest.createdAt})) inserted++;
    } else if (type==='agent') {
      if (await ingestAgentEvent(chatId,latest,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true})) inserted++;
    }
  }
  await db.markBootstrapped(chatId);
  return {inserted,processed};
}

function queueTrackedSummary(summaryOrId){
  const summary=(summaryOrId && typeof summaryOrId==='object') ? summaryOrId : {id:String(summaryOrId||'')};
  const id=String(summary?.id||'').trim();
  if(!id)return false;
  trackedChatIds.add(id);
  const previous=trackedSummaries.get(id);
  const merged=previous ? {...previous,...summary,id} : {...summary,id};
  trackedSummaries.set(id,merged);
  // Map is an unbounded-by-count deduplicating hot queue. PostgreSQL below is the
  // durable safety net, so a Railway restart cannot permanently discard discovered work.
  trackedQueue.set(id,merged);
  if(config.lcDurableQueueEnabled){
    durableBuffer.set(id,{chatId:id,summary:merged,summaryHash:summaryFingerprint(merged),priority:1000-Math.min(900,Math.max(0,Number(merged?.rank)||0))});
  }
  return true;
}

async function flushDurableBuffer(){
  if(!config.lcDurableQueueEnabled || durableFlushRunning || durableBuffer.size===0)return 0;
  durableFlushRunning=true;
  const picked=[];
  try{
    const max=Math.max(config.lcTrackedBatchSize,config.lcDurableQueueLoadSize);
    for(const [id,item] of durableBuffer){
      picked.push([id,item]); durableBuffer.delete(id);
      if(picked.length>=max)break;
    }
    if(!picked.length)return 0;
    await db.enqueueLiveChatSyncJobs(picked.map(([,item])=>item));
    return picked.length;
  }catch(e){
    for(const [id,item] of picked) if(!durableBuffer.has(id)) durableBuffer.set(id,item);
    await db.logError('poller','DURABLE_QUEUE_FLUSH_FAILED',e.message,{bufferDepth:durableBuffer.size}).catch(()=>{});
    return 0;
  }finally{ durableFlushRunning=false; }
}

async function hydrateTrackedFromDurable(){
  if(!config.lcDurableQueueEnabled)return 0;
  const room=Math.max(0,config.lcDurableQueueLoadSize-trackedQueue.size);
  if(room<=0)return 0;
  const jobs=await db.claimLiveChatSyncJobs(room,{leaseSeconds:config.lcDurableQueueLeaseSeconds});
  for(const job of jobs){
    const id=String(job?.chat_id||'').trim(); if(!id)continue;
    const base=(job?.summary && typeof job.summary==='object')?{...job.summary}:{id};
    base.id=String(base.id||id);
    trackedChatIds.add(id);
    const merged={...(trackedSummaries.get(id)||{}),...base,id};
    Object.defineProperty(merged,'__durableGeneration',{value:Number(job.generation),enumerable:false,configurable:true});
    Object.defineProperty(merged,'__durableHash',{value:String(job.summary_hash||''),enumerable:false,configurable:true});
    Object.defineProperty(merged,'__durableAttempts',{value:Number(job.attempts||1),enumerable:false,configurable:true});
    trackedSummaries.set(id,merged);
    trackedQueue.set(id,merged);
  }
  return jobs.length;
}

function removeTrackedChat(id){
  const chatId=String(id||'');
  trackedChatIds.delete(chatId);
  trackedSummaries.delete(chatId);
  trackedQueue.delete(chatId);
  durableBuffer.delete(chatId);
  summaryFingerprints.delete(chatId);
  if(config.lcDurableQueueEnabled) void db.completeLiveChatSyncJob(chatId).catch(()=>{});
}

function newPollStats(){
  return {newMessages:0,processed:0,fetched:0,unchanged:0,fetchErrors:0,bootstrapped:0,terminal:0,skippedBusy:0};
}

async function processSummaryForPoll(summary,rank,livechat,stats){
  if(!summary?.id)return {skipped:'missing_id'};
  const chatId=String(summary.id);
  if(processingChatIds.has(chatId)){ stats.skippedBusy++; return {skipped:'busy'}; }
  processingChatIds.add(chatId);
  trackedQueue.delete(chatId);
  trackedChatIds.add(chatId);
  trackedSummaries.set(chatId,{...(trackedSummaries.get(chatId)||{}),...summary,id:chatId});
  try{
    const fp=summaryFingerprint(summary);
    const oldFp=summaryFingerprints.get(chatId);
    const state={...livechat.chatState(summary),rank};
    await db.upsertConversation(summary,{visible:true,state});
    await db.updateTypingFromSummary(chatId,summary).catch(()=>{});
    const dbState=await db.getConversationState(chatId);

    // A current provider summary that has not changed does not need get_chat again.
    if(dbState?.bootstrapped_at && Number(dbState?.message_count||0)>0 && oldFp===fp){ stats.unchanged++; return {unchanged:true}; }

    let chat=summary;
    if(!Array.isArray(chat?.threads) || chat.threads.length===0){
      try{ chat=await livechat.getChat(chatId,summary); stats.fetched++; }
      catch(e){ stats.fetchErrors++; await db.logError('poller','GET_CHAT_FAILED',e.message,{chatId}).catch(()=>{}); return {error:e.message}; }
    }
    if(!chat?.id)return {skipped:'empty_detail'};

    // Provider-confirmed terminal detail is authoritative for this individual chat.
    // This is safe on the fast/tracked path because it does not infer closure from a
    // missing partial-page result; it uses the chat's own provider state.
    if(livechat.chatActiveFlag(chat)===false){
      removeTrackedChat(chatId);
      await db.markConversationEnded(chatId).catch(()=>{});
      stats.terminal++;
      return {terminal:true};
    }

    await db.upsertConversation(chat,{visible:true,state});
    const events=extractChatEvents(chat);
    if(!events.length){
      await db.clearBootstrapped(chatId);
      await db.logError('poller','EMPTY_CHAT_DETAIL','LiveChat get_chat returned no readable message events',{chatId,diagnostics:livechat.chatDiagnostics(chat)}).catch(()=>{});
      summaryFingerprints.delete(chatId);
      return {skipped:'empty_events'};
    }

    await ensureFreshWelcomeGreeting(chatId,events,livechat);
    if(!dbState?.bootstrapped_at || Number(dbState?.message_count||0)===0){
      const b=await bootstrapChat(chatId,chat,events,livechat);
      stats.newMessages+=b.inserted; stats.processed+=b.processed; stats.bootstrapped++;
      summaryFingerprints.set(chatId,fp);
      return {bootstrapped:true};
    }

    const unseen=[];
    for(const ev of events) if(!(await db.messageExists(chatId,ev.eventId))) unseen.push(ev);
    const newest=unseen.at(-1)||null;
    for(const ev of unseen){
      const type=senderType(ev,chat);
      const isNewest=newest && ev.eventId===newest.eventId;
      if(isWelcomeTriggerEvent(ev)){
        if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:false,allowGreetingTrigger:true})) stats.newMessages++;
      }else if(type==='customer' && isNewest){
        if(ageSeconds(ev.createdAt)*1000 < config.memberDebounceMs){ summaryFingerprints.delete(chatId); continue; }
        const result=await processCustomerMessage({chatId,eventId:ev.eventId,threadId:ev.threadId,text:ev.text,createdAt:ev.createdAt,livechat,attachments:ev.attachments||[]});
        if(!result?.skipped) stats.processed++;
        if(result?.skipped!=='duplicate') stats.newMessages++;
      }else if(type==='customer'){
        if(await db.insertMessage({chatId,eventId:ev.eventId,senderType:'customer',authorId:ev.authorId,text:ev.text,normalizedText:normalizeText(ev.text),intent:detectIntent(ev.text),createdAt:ev.createdAt,attachments:ev.attachments||[]})) stats.newMessages++;
      }else if(type==='agent'){
        if(await ingestAgentEvent(chatId,ev,chat,livechat,{allowTakeover:true,allowGreetingTrigger:true})) stats.newMessages++;
      }
    }
    await ensureFreshWelcomeGreeting(chatId,events,livechat);
    if(!newest || await db.messageExists(chatId,newest.eventId)) summaryFingerprints.set(chatId,fp);
    else summaryFingerprints.delete(chatId);
    return {ok:true,newEvents:unseen.length};
  }finally{
    processingChatIds.delete(chatId);
  }
}

export async function syncOnce(livechat,{manual=false}={}){
  if(inFlight)return {skipped:'already_running'};
  inFlight=true; lastError=null; const syncStarted=Date.now(); lastTick=new Date(syncStarted).toISOString();
  try{
    if(!manual){
      const enabled=Boolean(await db.getSetting('system_enabled',true));
      if(!enabled){ paused=true; lastSuccess=new Date().toISOString(); lastResult={ok:true,paused:true,skipped:'system_off'}; return lastResult; }
    }
    paused=false;
    const data=await livechat.listChats({paginate:false});
    const rawChats=data?._normalizedChats || data?.chats_summary || data?.chats || [];
    const chats=livechat.filterInbox(rawChats);
    const stats=newPollStats();
    const seenChatIds=[];
    const ranked=[];
    for(const [rank,summary] of chats.entries()){
      if(!summary?.id)continue;
      const chatId=String(summary.id);
      seenChatIds.push(chatId); ranked.push([rank,summary]);
      queueTrackedSummary({...summary,rank});
    }
    // Discovery is intentionally lightweight. Persist first, then let the dedicated
    // tracked worker perform get_chat + AI work with bounded concurrency. This prevents
    // one slow AI/provider request from blocking discovery of the next active chats.
    await flushDurableBuffer();
    await mapBounded(ranked,Math.min(16,config.lcTrackedConcurrency*2),async([rank,summary])=>{
      await db.upsertConversation(summary,{visible:true,state:{...livechat.chatState(summary),rank}})
        .catch(async e=>{stats.fetchErrors++;await db.logError('poller','FAST_DISCOVERY_UPSERT_FAILED',e.message,{chatId:String(summary.id)}).catch(()=>{});});
    });
    lastSuccess=new Date().toISOString();
    lastResult={ok:true,listSource:data?._listSource||'unknown',rawChats:rawChats.length,chats:chats.length,fetched:stats.fetched,unchanged:stats.unchanged,fetchErrors:stats.fetchErrors,bootstrapped:stats.bootstrapped,newMessages:stats.newMessages,processed:stats.processed,terminal:stats.terminal,pagesFetched:data?._pagesFetched||1,inventoryComplete:Boolean(data?._inventoryComplete),trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size,queued:ranked.length};
    await db.setIntegrationHealth('livechat_poll',{status:'OK',latencyMs:Date.now()-syncStarted,meta:{chats:chats.length,newMessages:stats.newMessages,processed:stats.processed,fetchErrors:stats.fetchErrors,trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size}}).catch(()=>{});
    return lastResult;
  }catch(e){
    lastError=e.message;
    await db.setIntegrationHealth('livechat_poll',{status:'ERROR',latencyMs:Date.now()-syncStarted,error:e.message,meta:{trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size}}).catch(()=>{});
    await db.logError('poller','SYNC_FAILED',e.message).catch(()=>{});
    throw e;
  }finally{ inFlight=false; }
}

async function mapBounded(items,concurrency,fn){
  const list=Array.from(items||[]); let next=0;
  const workers=Array.from({length:Math.min(Math.max(1,Number(concurrency)||1),list.length||1)},async()=>{
    while(true){ const i=next++; if(i>=list.length)return; await fn(list[i],i); }
  });
  await Promise.all(workers);
}

export async function deepSyncOnce(livechat){
  if(deepState.running)return {skipped:'already_running',generation:deepState.generation};
  const generation=deepState.generation+1;
  deepState.generation=generation; deepState.running=true; deepState.status='RUNNING'; deepState.lastError=null; deepState.inventoryComplete=false; deepState.pagesFetched=0; deepState.inventorySize=0;
  const started=Date.now();
  let discoveryRank=0;
  try{
    const data=await livechat.listChats({
      paginate:true,maxPages:10000,retries:2,pageDelayMs:50,
      onPage:async({items,pageIndex})=>{
        if(generation!==deepState.generation){ const e=new Error('STALE_DEEP_GENERATION'); e.code='STALE_DEEP_GENERATION'; throw e; }
        const pageItems=Array.isArray(items)?items:[];
        deepState.pagesFetched=Math.max(deepState.pagesFetched,Number(pageIndex)||0);
        deepState.inventorySize+=pageItems.length;
        const pageActive=livechat.filterInbox(pageItems);
        // Progressive discovery: active chats become visible and enter the processing
        // queue as soon as their provider page is seen. We still do NO destructive
        // reconciliation until the full cursor chain completes successfully.
        await mapBounded(pageActive,8,async summary=>{
          if(!summary?.id)return;
          const rank=discoveryRank++;
          queueTrackedSummary(summary);
          // Queueing must survive an isolated DB write failure. The tracked worker will
          // retry the normal upsert/detail path; one bad row must not abort provider pagination.
          await db.upsertConversation(summary,{visible:true,state:{...livechat.chatState(summary),rank}})
            .catch(async e=>{ await db.logError('poller','DEEP_PROGRESSIVE_UPSERT_FAILED',e.message,{chatId:String(summary.id)}).catch(()=>{}); });
        });
      }
    });
    const rawChats=data?._normalizedChats || data?.chats_summary || data?.chats || [];
    deepState.pagesFetched=Number(data?._pagesFetched||deepState.pagesFetched||1);
    deepState.inventorySize=rawChats.length;
    if(data?._inventoryComplete!==true){
      const e=new Error(`LIVECHAT_INVENTORY_PARTIAL:${data?._paginationStopReason||'unknown'}`); e.code='LIVECHAT_INVENTORY_PARTIAL'; throw e;
    }
    if(generation!==deepState.generation){ deepState.status='IDLE'; return {skipped:'stale_generation',generation}; }

    const active=livechat.filterInbox(rawChats);
    const activeIds=[...new Set(active.map(x=>String(x?.id||'')).filter(Boolean))];
    const activeSet=new Set(activeIds);
    const providerById=new Map(rawChats.filter(x=>x?.id!=null).map(x=>[String(x.id),x]));

    const visibleBefore=await db.getVisibleInboxConversationIds();
    const terminal=[];
    for(const id of visibleBefore){
      if(activeSet.has(id))continue;
      const summary=providerById.get(id);
      if(summary && livechat.chatActiveFlag(summary)===false)terminal.push(id);
    }
    await mapBounded(terminal,4,async id=>{ removeTrackedChat(id); await db.markConversationEnded(id); });
    const hidden=await db.reconcileInboxVisibilityAuthoritative(activeIds);

    // Full authoritative generation replaces the tracked set, but the processing
    // queue itself has no count cap. Active chats outside the first 100 remain queued.
    trackedChatIds.clear();
    for(const summary of active){ queueTrackedSummary(summary); }
    for(const id of [...trackedSummaries.keys()]) if(!activeSet.has(id)) removeTrackedChat(id);
    for(const id of [...summaryFingerprints.keys()]) if(!activeSet.has(id)) summaryFingerprints.delete(id);

    deepState.completedGeneration=generation; deepState.lastSuccess=new Date().toISOString(); deepState.inventoryComplete=true; deepState.status='COMPLETED'; deepState.lastDurationMs=Date.now()-started;
    const visibleAfter=await db.getVisibleInboxConversationIds();
    const meta={ok:true,rawChats:rawChats.length,deduplicatedChats:rawChats.length,pagesFetched:deepState.pagesFetched,inventorySize:rawChats.length,inventoryComplete:true,myActiveChats:activeIds.length,providerMyActiveChats:activeIds.length,expectedVisibleInbox:activeIds.length,actualVisibleInbox:visibleAfter.length,inboxMismatch:visibleAfter.length-activeIds.length,deepGeneration:generation,deepCompletedGeneration:generation,deepSyncRunning:false,reconciledClosed:terminal.length,reconciledHidden:hidden.length,trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size,durationMs:deepState.lastDurationMs};
    await db.setIntegrationHealth('livechat_discovery',{status:'OK',latencyMs:deepState.lastDurationMs,meta}).catch(()=>{});
    return meta;
  }catch(e){
    if(e?.code==='STALE_DEEP_GENERATION'){ deepState.status='IDLE'; return {skipped:'stale_generation',generation}; }
    deepState.lastError=String(e?.message||e); deepState.inventoryComplete=false; deepState.status='FAILED'; deepState.lastDurationMs=Date.now()-started;
    await db.setIntegrationHealth('livechat_discovery',{status:'ERROR',latencyMs:deepState.lastDurationMs,error:deepState.lastError,meta:{pagesFetched:deepState.pagesFetched,inventorySize:deepState.inventorySize,inventoryComplete:false,deepGeneration:generation,deepCompletedGeneration:deepState.completedGeneration,trackedChats:trackedChatIds.size,trackedQueueDepth:trackedQueue.size}}).catch(()=>{});
    throw e;
  }finally{
    deepState.running=false;
    if(deepState.status==='COMPLETED')deepState.status='IDLE';
  }
}


async function drainTrackedQueue(livechat){
  if(trackedWorkerRunning)return {processed:0,remaining:trackedQueue.size};
  trackedWorkerRunning=true;
  const batch=[];
  try{
    await flushDurableBuffer();
    if(trackedQueue.size<config.lcTrackedBatchSize) await hydrateTrackedFromDurable().catch(async e=>{await db.logError('poller','DURABLE_QUEUE_CLAIM_FAILED',e.message).catch(()=>{});});
    if(trackedQueue.size===0)return {processed:0,remaining:0};
    // Batch/concurrency are throughput controls, never active-chat count caps. Jobs
    // remain in PostgreSQL until acknowledged after successful idempotent processing.
    for(const [id,summary] of trackedQueue){
      if(processingChatIds.has(id))continue;
      batch.push(summary);
      if(batch.length>=config.lcTrackedBatchSize)break;
    }
    const stats=newPollStats();
    let failed=0;
    await mapBounded(batch,config.lcTrackedConcurrency,async(summary,index)=>{
      const chatId=String(summary?.id||'');
      const generation=summary?.__durableGeneration ?? null;
      const summaryHash=summary?.__durableHash || summaryFingerprint(summary);
      try{
        const result=await processSummaryForPoll(summary,index,livechat,stats);
        const retryable=Boolean(result?.error || ['busy','empty_detail','empty_events'].includes(String(result?.skipped||'')));
        if(retryable){
          failed++;
          if(config.lcDurableQueueEnabled){
            const attempts=Math.max(1,Number(summary?.__durableAttempts||1));
            const delay=Math.min(300000,500*(2**Math.min(8,attempts)));
            await db.retryLiveChatSyncJob(chatId,{generation,error:String(result?.error||result?.skipped||'retryable_sync_result'),delayMs:delay}).catch(()=>{});
          }
          return;
        }
        if(config.lcDurableQueueEnabled) await db.completeLiveChatSyncJob(chatId,generation!=null?{generation}:{summaryHash});
      }catch(e){
        failed++;
        if(config.lcDurableQueueEnabled){
          const attempts=Math.max(1,Number(summary?.__durableAttempts||1));
          const delay=Math.min(300000,500*(2**Math.min(8,attempts)));
          await db.retryLiveChatSyncJob(chatId,{generation,error:e.message,delayMs:delay}).catch(()=>{});
        }
        await db.logError('poller','TRACKED_CHAT_PROCESS_FAILED',e.message,{chatId}).catch(()=>{});
      }
    });
    return {processed:batch.length-failed,failed,remaining:trackedQueue.size,stats};
  }finally{ trackedWorkerRunning=false; }
}

async function seedTrackedFromDb(){
  try{
    const ids=await db.getVisibleInboxConversationIds();
    for(const id of ids)queueTrackedSummary({id});
    return ids.length;
  }catch(e){
    await db.logError('poller','TRACKED_SEED_FAILED',e.message).catch(()=>{});
    return 0;
  }
}

function scheduleTrackedWorker(livechat,delay=250){
  if(!loopStarted)return;
  if(trackedTimer)clearTimeout(trackedTimer);
  trackedTimer=setTimeout(async()=>{
    trackedTimer=null;
    try{await drainTrackedQueue(livechat);}catch(e){await db.logError('poller','TRACKED_WORKER_FAILED',e.message).catch(()=>{});}
    finally{
      if(loopStarted)scheduleTrackedWorker(livechat,trackedQueue.size>0?250:1000);
    }
  },Math.max(0,delay));
  trackedTimer.unref?.();
}

function scheduleDeep(livechat,delay){
  if(!loopStarted)return;
  if(deepTimer)clearTimeout(deepTimer);
  deepTimer=setTimeout(async()=>{
    deepTimer=null;
    try{await deepSyncOnce(livechat);}catch(e){await db.logError('poller','DEEP_SYNC_FAILED',e.message).catch(()=>{});}
    finally{ if(loopStarted)scheduleDeep(livechat,Math.max(30000,config.lcPollMs*30)); }
  },Math.max(0,delay));
  deepTimer.unref?.();
}

export function startPoller(livechat){
  if (config.lcSyncMode!=='polling' || loopStarted) return;
  loopStarted=true;
  const run=async()=>{
    if(!loopStarted)return;
    try{await syncOnce(livechat);}catch{} finally{
      if(loopStarted){timer=setTimeout(run,config.lcPollMs);timer.unref?.();}
    }
  };
  void seedTrackedFromDb();
  void run();
  scheduleTrackedWorker(livechat,250);
  scheduleDeep(livechat,500);
}
export async function stopPoller({waitMs=5000}={}){
  loopStarted=false;
  if(timer)clearTimeout(timer); timer=null;
  if(deepTimer)clearTimeout(deepTimer); deepTimer=null;
  if(trackedTimer)clearTimeout(trackedTimer); trackedTimer=null;
  // Invalidate any deep result that completes after shutdown/restart.
  deepState.generation++;
  const until=Date.now()+waitMs;
  while((inFlight||deepState.running||trackedWorkerRunning||durableFlushRunning)&&Date.now()<until)await new Promise(r=>setTimeout(r,50));
  return !inFlight&&!deepState.running&&!trackedWorkerRunning&&!durableFlushRunning;
}
