import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

test('End Chat uses documented v3.6 deactivate_chat payload only',()=>{
  const src=fs.readFileSync(new URL('../src/livechat.js',import.meta.url),'utf8');
  const start=src.indexOf('async endChat(chatId)');
  const end=src.indexOf('async prepareImageAttachments',start);
  const block=src.slice(start,end);
  assert.match(block,/this\.call\('deactivate_chat',\{id,ignore_requester_presence:true\}\)/);
  assert.doesNotMatch(block,/chat_id:id/);
});

test('End Chat is idempotent for already inactive or missing chats',()=>{
  const src=fs.readFileSync(new URL('../src/livechat.js',import.meta.url),'utf8');
  const start=src.indexOf('async endChat(chatId)');
  const end=src.indexOf('async prepareImageAttachments',start);
  const block=src.slice(start,end);
  assert.match(block,/alreadyClosed:true/);
  assert.match(block,/Number\(e\?\.status\)===404/);
  assert.match(block,/chat is inactive/);
});

test('dashboard closes local conversation only after LC endChat succeeds',()=>{
  const src=fs.readFileSync(new URL('../src/server.js',import.meta.url),'utf8');
  const routePos=src.indexOf("app.post('/api/conversations/:id/end'");
  const lcPos=src.indexOf('const lcResult=await lc.endChat(req.params.id)',routePos);
  const localPos=src.indexOf('const local=await markConversationEnded(req.params.id)',routePos);
  assert.ok(routePos>=0); assert.ok(lcPos>routePos); assert.ok(localPos>lcPos);
});
