import crypto from 'node:crypto';
import { config } from './config.js';
import * as db from './db.js';
import { syncChatById, forgetChat } from './poller.js';
import { normalizeLiveChatWebhook } from './livechat-webhook.js';

let timer=null,running=false,inFlight=0,lastRunAt=null,lastSuccessAt=null,lastError=null,processedTotal=0,retriedTotal=0,deadTotal=0;
const workerId=`${process.pid}:${crypto.randomUUID().slice(0,8)}`;

function text(v){return String(v??'').trim();}

export { normalizeLiveChatWebhook } from './livechat-webhook.js';

export async function enqueueLiveChatWebhook(body={}){
  const n=normalizeLiveChatWebhook(body);
  if(!n.chatId) return {accepted:false,ignored:true,reason:'missing_chat_id',normalized:n};
  if(n.deactivated){
    const key=`webhook:closed:${n.chatId}:${n.threadId||'thread'}`;
    const job=await db.enqueueLiveChatIngressJob({dedupeKey:key,jobType:'CLOSE_CHAT',chatId:n.chatId,threadId:n.threadId||null,eventId:n.eventId||null,payload:{action:n.action},priority:100});
    // Close the local send gate immediately. Archiving and the remaining lifecycle work
    // stay durable in CLOSE_CHAT, but an already-running AI generation can no longer
    // emit a message after LiveChat has told us the thread is closed.
    const locallyClosed=await db.markConversationInactive(n.chatId,{threadId:n.threadId||null}).catch(()=>false);
    if(locallyClosed){
      forgetChat(n.chatId);
      if(job?.id) await db.cancelLiveChatIngressJobs(n.chatId,job.id).catch(()=>{});
    }
    return {accepted:true,deduped:!job,jobId:job?.id||null,jobType:'CLOSE_CHAT'};
  }
  const eventKey=n.eventId||crypto.createHash('sha1').update(JSON.stringify(n.raw||{})).digest('hex');
  const key=`webhook:${n.action}:${n.chatId}:${eventKey}`;
  const job=await db.enqueueLiveChatIngressJob({dedupeKey:key,jobType:'SYNC_CHAT',chatId:n.chatId,threadId:n.threadId||null,eventId:n.eventId||null,payload:{action:n.action,summary:n.chat||null},delayMs:Math.min(config.memberDebounceMs,2500),priority:80});
  return {accepted:true,deduped:!job,jobId:job?.id||null,jobType:'SYNC_CHAT'};
}

function retryable(err){
  const status=Number(err?.status||0);
  if(err?.name==='AbortError') return true;
  if([408,409,425,429,500,502,503,504].includes(status)) return true;
  return /timeout|ECONN|ENET|EAI_AGAIN|fetch failed|temporar/i.test(text(err?.message));
}
function backoff(attempt){const base=Math.max(250,Number(config.lcIngressRetryBaseMs||1000));return Math.min(60000,base*(2**Math.max(0,attempt-1))+Math.floor(Math.random()*base));}

async function handleJob(job,livechat){
  const type=text(job.job_type).toUpperCase(); const chatId=text(job.chat_id);
  if(type==='CLOSE_CHAT'){
    if(chatId){
      const ended=await db.markConversationEnded(chatId,{threadId:job.thread_id||null,reason:'LIVECHAT_DEACTIVATED'});
      if(!ended?.ignored){forgetChat(chatId);await db.cancelLiveChatIngressJobs(chatId,job.id);}
      return ended?.ignored?{closed:false,staleThread:true}:{closed:true};
    }
    return {closed:true};
  }
  if(type==='SYNC_CHAT'){
    if(!chatId) throw new Error('LIVECHAT_INGRESS_CHAT_ID_REQUIRED');
    const summary=job?.payload?.summary&&typeof job.payload.summary==='object'?job.payload.summary:{};
    try{return await syncChatById(livechat,chatId,summary);}catch(e){
      if(Number(e?.status)===404 || /chat is inactive|inactive/i.test(text(e?.message))){await db.markConversationEnded(chatId);return {closed:true};}
      throw e;
    }
  }
  throw new Error(`LIVECHAT_INGRESS_UNKNOWN_JOB:${type}`);
}

async function processJob(job,livechat){
  try{
    const result=await handleJob(job,livechat);
    if(result?.deferredMs){await db.retryLiveChatIngressJob(job.id,'MEMBER_DEBOUNCE',result.deferredMs);retriedTotal++;return;}
    await db.finishLiveChatIngressJob(job.id); processedTotal++; lastSuccessAt=new Date().toISOString();
  }catch(e){
    const attempts=Number(job.attempts||1); const transient=retryable(e); const dead=!transient||attempts>=config.lcIngressMaxAttempts;
    if(dead){deadTotal++;await db.addDeadLetter({source:'LIVECHAT_INGRESS',eventKey:job.dedupe_key,chatId:job.chat_id,payload:job.payload,error:text(e?.message),attempts}).catch(()=>{});}
    else retriedTotal++;
    await db.retryLiveChatIngressJob(job.id,e?.message||String(e),dead?0:backoff(attempts),{dead});
    lastError=text(e?.message);
  }
}

async function tick(livechat){
  if(running)return; running=true; lastRunAt=new Date().toISOString();
  try{
    const jobs=await db.claimLiveChatIngressJobs(config.lcIngressBatchSize,{workerId,staleSeconds:config.lcIngressStaleSeconds});
    let cursor=0; const count=Math.min(config.lcIngressConcurrency,jobs.length||1);
    const workers=Array.from({length:count},async()=>{while(true){const i=cursor++;if(i>=jobs.length)break;inFlight++;try{await processJob(jobs[i],livechat);}finally{inFlight--;}}});
    await Promise.all(workers);
  }catch(e){lastError=text(e?.message);await db.logError('livechat_ingress','WORKER_TICK_FAILED',lastError).catch(()=>{});}finally{running=false;}
}

export function startLiveChatIngressWorker(livechat){
  if(timer)return;
  const loop=async()=>{await tick(livechat);timer=setTimeout(loop,config.lcIngressWorkerMs);timer.unref?.();};
  timer=setTimeout(loop,250);timer.unref?.();
}
export async function stopLiveChatIngressWorker({waitMs=7000}={}){if(timer)clearTimeout(timer);timer=null;const until=Date.now()+waitMs;while((running||inFlight)&&Date.now()<until)await new Promise(r=>setTimeout(r,50));return !running&&inFlight===0;}
export async function liveChatIngressStatus(){const q=await db.liveChatIngressStats().catch(()=>({pending:null,processing:null,dead:null,oldest_job_age_ms:null}));return {running:Boolean(timer),tickRunning:running,inFlight,workerId,lastRunAt,lastSuccessAt,lastError,processedTotal,retriedTotal,deadTotal,queue:q};}
