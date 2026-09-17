import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import {LiveChatClient} from '../src/livechat.js';

function lcError(status,message){
  const e=new Error(`LIVECHAT_${status}: ${message}`);
  e.status=status;
  return e;
}

test('v1.32.1 sendMessage repairs requester membership on the specific LiveChat 403 and retries once',async()=>{
  const calls=[];
  const lc=new LiveChatClient({base:'http://local',accountId:'agent@example.com',requesterUserId:'agent@example.com',pat:'p'});
  let sends=0;
  lc.call=async(action,body,options)=>{
    calls.push({action,body,options});
    if(action==='send_event' && sends++===0) throw lcError(403,'Requester is not user of the chat');
    if(action==='add_user_to_chat') return {};
    return {event_id:'out-1'};
  };
  const out=await lc.sendMessage('c1','halo');
  assert.equal(out.event_id,'out-1');
  assert.equal(calls.length,3);
  assert.deepEqual(calls[0],{action:'send_event',body:{chat_id:'c1',event:{type:'message',text:'halo',visibility:'all'}},options:{retries:0}});
  assert.deepEqual(calls[1],{action:'add_user_to_chat',body:{chat_id:'c1',user_id:'agent@example.com',user_type:'agent',visibility:'all',ignore_requester_presence:true},options:{retries:0}});
  assert.deepEqual(calls[2],{action:'send_event',body:{chat_id:'c1',event:{type:'message',text:'halo',visibility:'all'}},options:{retries:0}});
});

test('v1.32.1 requester recovery applies to file events too',async()=>{
  const calls=[];
  const lc=new LiveChatClient({base:'http://local',accountId:'agent@example.com',pat:'p'});
  let sends=0;
  lc.call=async(action,body,options)=>{
    calls.push({action,body,options});
    if(action==='send_event' && sends++===0) throw lcError(403,'Requester is not user of the chat');
    if(action==='add_user_to_chat') return {};
    return {event_id:'file-1'};
  };
  const out=await lc.sendFile('c1',{url:'https://cdn.example/proof.png'});
  assert.equal(out.event_id,'file-1');
  assert.equal(calls.map(x=>x.action).join(','),'send_event,add_user_to_chat,send_event');
});

test('v1.32.1 arbitrary 403 is never hidden as a membership problem',async()=>{
  const calls=[];
  const lc=new LiveChatClient({base:'http://local',accountId:'agent@example.com',pat:'p'});
  lc.call=async(action,body,options)=>{calls.push({action,body,options});throw lcError(403,'Authorization error');};
  await assert.rejects(()=>lc.sendMessage('c1','halo'),/Authorization error/);
  assert.equal(calls.length,1);
  assert.equal(calls[0].action,'send_event');
});

test('v1.32.1 already-present race during membership recovery is idempotent',async()=>{
  const lc=new LiveChatClient({base:'http://local',accountId:'agent@example.com',pat:'p'});
  lc.call=async(action)=>{
    assert.equal(action,'add_user_to_chat');
    throw lcError(409,'Agent already exists in chat');
  };
  const out=await lc.ensureRequesterInChat('c1');
  assert.equal(out.ok,true);
  assert.equal(out.alreadyPresent,true);
});



test('v1.32.1 real HTTP path handles LiveChat 403 membership recovery without duplicate retry loop',async()=>{
  const seen=[];
  let sendCount=0;
  const server=http.createServer(async(req,res)=>{
    let body=''; for await(const chunk of req) body+=chunk;
    const action=String(req.url||'').split('/').at(-1);
    const json=body?JSON.parse(body):{};
    seen.push({action,json,authorization:req.headers.authorization});
    res.setHeader('content-type','application/json');
    if(action==='send_event' && sendCount++===0){
      res.statusCode=403;
      res.end(JSON.stringify({error:{type:'authorization',message:'Requester is not user of the chat'}}));
      return;
    }
    if(action==='add_user_to_chat'){res.statusCode=200;res.end('{}');return;}
    res.statusCode=200;res.end(JSON.stringify({event_id:'http-out-1'}));
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try{
    const {port}=server.address();
    const lc=new LiveChatClient({base:`http://127.0.0.1:${port}/v3.6/agent/action`,accountId:'agent@example.com',pat:'secret',timeoutMs:3000});
    const out=await lc.sendMessage('chat-1','halo');
    assert.equal(out.event_id,'http-out-1');
    assert.equal(seen.map(x=>x.action).join(','),'send_event,add_user_to_chat,send_event');
    assert.deepEqual(seen[1].json,{chat_id:'chat-1',user_id:'agent@example.com',user_type:'agent',visibility:'all',ignore_requester_presence:true});
    assert.equal(seen[0].authorization,'Basic '+Buffer.from('agent@example.com:secret').toString('base64'));
  }finally{
    await new Promise(resolve=>server.close(resolve));
  }
});

test('v1.32.1 health state is reset on boot and stale legacy integration names are hidden',()=>{
  const db=fs.readFileSync(new URL('../src/db.js',import.meta.url),'utf8');
  const server=fs.readFileSync(new URL('../src/server.js',import.meta.url),'utf8');
  assert.match(db,/export async function resetIntegrationHealth\(\)/);
  assert.match(db,/DELETE FROM integration_health/);
  assert.match(db,/livechat_discovery/);
  assert.match(db,/livechat_send_message/);
  assert.match(db,/livechat_reconciliation/);
  assert.match(server,/health-reset[\s\S]*resetIntegrationHealth\(\)/);
});

test('v1.32.1 operations health cannot report ok true while an integration is ERROR',()=>{
  const server=fs.readFileSync(new URL('../src/server.js',import.meta.url),'utf8');
  const start=server.indexOf("app.get('/api/ops/health'");
  const end=server.indexOf("app.get('/api/ops/dead-letters'",start);
  const block=server.slice(start,end);
  assert.match(block,/status\|\|''\)\.toUpperCase\(\)==='ERROR'/);
  assert.match(block,/ok:errors\.length===0/);
  assert.doesNotMatch(block,/res\.json\(\{ok:true/);
});

test('v1.32.2 Railway env documents requester user id and production UI checks runtime LiveChat health',()=>{
  const env=fs.readFileSync(new URL('../.env.example',import.meta.url),'utf8');
  const config=fs.readFileSync(new URL('../src/config.js',import.meta.url),'utf8');
  const health=fs.readFileSync(new URL('../public/assets/js/pages/health.js',import.meta.url),'utf8');
  assert.match(env,/^LIVECHAT_REQUESTER_USER_ID=$/m);
  assert.match(config,/lcRequesterUserId:\s*process\.env\.LIVECHAT_REQUESTER_USER_ID \|\| ''/);
  assert.match(health,/livechat_poll','livechat_deep_sync','livechat/);
  assert.match(health,/\.some\(x=>String\(x\.status/);
});
