function text(v){return String(v??'').trim();}
function actionName(body={}){return text(body.action||body.type||body.webhook_type||body.event_type||body.name||body?.payload?.action).toLowerCase();}
function payloadOf(body={}){return body?.payload&&typeof body.payload==='object'?body.payload:body;}
function eventOf(body={}){const p=payloadOf(body);return p?.event||body?.event||null;}
function chatOf(body={}){const p=payloadOf(body);return p?.chat||body?.chat||null;}
function idsOf(body={}){
  const p=payloadOf(body),ev=eventOf(body),chat=chatOf(body);
  return {chatId:text(p?.chat_id||body?.chat_id||chat?.id),threadId:text(p?.thread_id||body?.thread_id||chat?.thread?.id||chat?.last_thread_summary?.id),eventId:text(ev?.id||p?.event_id||body?.event_id)};
}
export function normalizeLiveChatWebhook(body={}){
  const action=actionName(body); const p=payloadOf(body); const ids=idsOf(body); const chat=chatOf(body); const ev=eventOf(body);
  const deactivated=action.includes('chat_deactivated')||action.includes('deactivate_chat')||action==='closed'||p?.active===false||chat?.thread?.active===false;
  const incoming=action.includes('incoming_event')||action.includes('incoming_chat')||Boolean(ev)||Boolean(chat);
  return {action:action||'unknown',...ids,chat,event:ev,payload:p,deactivated,incoming,raw:body};
}
