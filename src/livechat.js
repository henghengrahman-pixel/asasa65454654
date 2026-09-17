import { config } from './config.js';

export class LiveChatClient {
  constructor(overrides={}) {
    this.base = overrides.base || config.lcApiBase;
    this.accountId = overrides.accountId || config.lcAccountId;
    this.pat = overrides.pat || config.lcPat;
    this.timeoutMs = overrides.timeoutMs || 15000;
    this.inboxMode = overrides.inboxMode || config.lcInboxMode;
  }
  ready() { return Boolean(this.accountId && this.pat && this.base); }
  authHeader() {
    return 'Basic ' + Buffer.from(`${this.accountId}:${this.pat}`).toString('base64');
  }
  async call(action, body={}, options={}) {
    if (!this.ready()) throw new Error('LIVECHAT_CREDENTIALS_MISSING');
    const retryableAction=new Set(['list_chats','list_threads','get_chat','list_archives','deactivate_chat','follow_chat','unfollow_chat']);
    const configured=Math.max(0,Number(options.retries ?? config.lcHttpRetries ?? 0));
    const maxRetries=retryableAction.has(String(action)) ? configured : 0;
    let attempt=0;
    while(true){
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
      try {
        const r = await fetch(`${this.base}/${action}`, {
          method:'POST',
          headers:{ 'Authorization': this.authHeader(), 'Content-Type':'application/json' },
          body: JSON.stringify(body),
          signal: ctrl.signal
        });
        const txt = await r.text();
        let data; try { data = txt ? JSON.parse(txt) : {}; } catch { data = { raw:txt }; }
        if (!r.ok) {
          const err = new Error(`LIVECHAT_${r.status}: ${data?.error?.message || data?.message || txt.slice(0,300)}`);
          err.status = r.status; err.data = data;
          const retryAfter=Number(r.headers.get('retry-after'));
          err.retryAfterMs=Number.isFinite(retryAfter)&&retryAfter>0?retryAfter*1000:null;
          throw err;
        }
        return data;
      } catch(err) {
        const transient=err?.name==='AbortError' || [408,425,429,500,502,503,504].includes(Number(err?.status));
        if(!transient || attempt>=maxRetries) throw err;
        const base=Math.max(100,Number(config.lcHttpRetryBaseMs||500));
        const backoff=err?.retryAfterMs || Math.min(15000,base*(2**attempt)+Math.floor(Math.random()*base));
        attempt++;
        await new Promise(r=>setTimeout(r,backoff));
      } finally { clearTimeout(timer); }
    }
  }

  // LiveChat Agent Chat API list_chats may expose the list as chats_summary.
  // Keep compatibility with alternate/older shapes as well.
  normalizeChatList(data) {
    if (Array.isArray(data?.chats_summary)) return { items:data.chats_summary, source:'chats_summary' };
    if (Array.isArray(data?.chats)) return { items:data.chats, source:'chats' };
    if (Array.isArray(data?.items)) return { items:data.items, source:'items' };
    return { items:[], source:'none' };
  }


  chatState(summary) {
    const th = summary?.last_thread_summary || summary?.last_thread || summary?.thread || (Array.isArray(summary?.threads)?summary.threads.at(-1):null) || {};
    const followed = summary?.is_followed;
    const active = typeof th?.active === 'boolean' ? th.active
      : (typeof summary?.active === 'boolean' ? summary.active
      : (String(summary?.status || '').toLowerCase() === 'active' ? true : null));
    const routingStatus = String(summary?.routing_status || th?.routing_status || '').toLowerCase();
    return { followed, active, routingStatus };
  }

  isMyActiveChat(summary) {
    const st = this.chatState(summary);
    // LiveChat's Agent API marks chats followed by the current agent with is_followed.
    // This most closely mirrors the web app's "My chats" list.
    if (st.followed === true) return st.active !== false && st.routingStatus !== 'closed';
    if (st.followed === false) return false;
    // Defensive fallback for response variants without is_followed.
    return st.active === true && !['closed','archived'].includes(st.routingStatus);
  }

  filterInbox(items) {
    if (this.inboxMode === 'all') return items;
    if (this.inboxMode === 'all_active') return items.filter(x=>this.chatState(x).active!==false);
    return items.filter(x => this.isMyActiveChat(x));
  }

  async listChats({pageId=null,activeOnly=true,limit=config.lcListLimit}={}) {
    // Agent Chat API v3.6 pagination contract: the first request carries filters/limit/sort_order;
    // every subsequent request carries page_id only. `filters.active`, not legacy `include_active`,
    // is the authoritative active-chat filter.
    const body=pageId
      ? {page_id:String(pageId)}
      : {filters:{active:activeOnly?true:null,include_chats_without_threads:true},sort_order:'desc',limit:Math.min(100,Math.max(1,Number(limit)||100))};
    const data=await this.call('list_chats',body);
    const normalized=this.normalizeChatList(data);
    return {...data,_normalizedChats:normalized.items,_listSource:normalized.source,_requestBody:body};
  }

  async listAllActiveChats(){
    const items=[]; const seenChats=new Set(); const seenPages=new Set();
    let pageId=null,pages=0,foundChats=null;
    do{
      if(pageId && seenPages.has(String(pageId))) throw new Error('LIVECHAT_PAGINATION_LOOP');
      if(pageId) seenPages.add(String(pageId));
      const data=await this.listChats({pageId,activeOnly:true,limit:100});
      pages++;
      if(foundChats==null && Number.isFinite(Number(data?.found_chats))) foundChats=Number(data.found_chats);
      for(const chat of data?._normalizedChats||[]){
        const id=String(chat?.id||'').trim();
        if(!id || seenChats.has(id)) continue;
        seenChats.add(id); items.push(chat);
      }
      pageId=data?.next_page_id ? String(data.next_page_id) : null;
    }while(pageId);
    return {items,pages,foundChats,complete:true};
  }
  normalizeChatDetail(data, fallback={}) {
    let chat = null;
    let source = 'none';
    if (data && typeof data === 'object' && data.chat && typeof data.chat === 'object') {
      chat = data.chat; source = 'chat';
    } else if (data && typeof data === 'object' && (data.id || Array.isArray(data.threads))) {
      chat = data; source = 'direct';
    } else if (Array.isArray(data?.chats) && data.chats[0]) {
      chat = data.chats[0]; source = 'chats[0]';
    } else if (Array.isArray(data?.items) && data.items[0]) {
      chat = data.items[0]; source = 'items[0]';
    }
    if (!chat) chat = { ...fallback };
    else chat = { ...fallback, ...chat };
    if (!chat.id && fallback?.id) chat.id = fallback.id;
    Object.defineProperty(chat, '_detailSource', { value: source, enumerable: false, configurable: true });
    return chat;
  }

  async getChat(chatId, fallback={}) {
    const id=String(chatId||'').trim();
    if(!id){const er=new Error('LIVECHAT_CHAT_ID_REQUIRED');er.status=400;throw er;}
    const threadId=fallback?.last_thread_summary?.id ?? fallback?.last_thread?.id ?? fallback?.thread?.id ?? fallback?.thread_id ?? null;
    const candidates=[{chat_id:id}];
    if(threadId!==null && threadId!==undefined && String(threadId)!=='') candidates.push({chat_id:id,thread_id:String(threadId)});
    let lastErr=null,best=null,bestCount=-1;
    for(let i=0;i<candidates.length;i++){
      const body=candidates[i];
      try{
        const data=await this.call('get_chat',body);
        const chat=this.normalizeChatDetail(data,fallback);
        Object.defineProperty(chat,'_getChatRequest',{value:body,enumerable:false,configurable:true});
        const count=extractChatEvents(chat).length;
        if(count>bestCount){best=chat;bestCount=count;}
        // v3.6 get_chat without thread_id already returns the latest thread. If it has
        // readable events, querying that same thread again by ID only doubles API load.
        if(i===0 && count>0) return chat;
      }catch(e){lastErr=e;if(![400,404,422].includes(Number(e?.status))) throw e;}
    }
    if(best) return best;
    if(lastErr) throw lastErr;
    return this.normalizeChatDetail({},fallback);
  }

  chatDiagnostics(chat) {
    const threads = Array.isArray(chat?.threads) ? chat.threads : [];
    const topEvents = Array.isArray(chat?.events) ? chat.events.length : 0;
    const threadEvents = threads.reduce((n,t)=>n + (Array.isArray(t?.events)?t.events.length:0), 0);
    const messages = extractChatEvents(chat).length;
    return {
      detailSource: chat?._detailSource || 'unknown',
      threadCount: threads.length,
      eventCount: topEvents + threadEvents,
      messageCount: messages,
      requestedThreadId: chat?._getChatRequest?.thread_id ?? null,
      requestUsed: chat?._getChatRequest || null,
      keys: chat && typeof chat==='object' ? Object.keys(chat).slice(0,30) : []
    };
  }
  sendMessage(chatId, text, {customId=null}={}) {
    const event={type:'message',text,visibility:'all'};
    if(customId) event.custom_id=String(customId).slice(0,128);
    return this.call('send_event',{chat_id:chatId,event},{retries:0});
  }

  async uploadFile(chatId, bytes, {name='image.jpg', contentType='image/jpeg'}={}) {
    if (!this.ready()) throw new Error('LIVECHAT_CREDENTIALS_MISSING');
    const id=String(chatId||'').trim();
    if(!id){ const er=new Error('LIVECHAT_CHAT_ID_REQUIRED'); er.status=400; throw er; }
    const data=Buffer.isBuffer(bytes)?bytes:Buffer.from(bytes||[]);
    if(!data.length){ const er=new Error('LIVECHAT_FILE_EMPTY'); er.status=400; throw er; }

    const form=new FormData();
    form.append('file',new Blob([data],{type:String(contentType||'application/octet-stream')}),String(name||'file'));
    const ctrl=new AbortController();
    const timer=setTimeout(()=>ctrl.abort(),Math.max(this.timeoutMs,20000));
    try{
      const r=await fetch(`${this.base}/upload_file`,{
        method:'POST',
        headers:{'Authorization':this.authHeader()},
        body:form,
        signal:ctrl.signal
      });
      const txt=await r.text();
      let out; try{out=txt?JSON.parse(txt):{};}catch{out={raw:txt};}
      if(!r.ok){
        const er=new Error(`LIVECHAT_${r.status}: ${out?.error?.message||out?.message||txt.slice(0,300)}`);
        er.status=r.status; er.data=out; throw er;
      }
      const file=out?.file || out?.files?.[0] || out?.uploaded_file || out;
      const url=String(file?.url||file?.file_url||file?.download_url||out?.url||'').trim();
      if(!url){ const er=new Error('LIVECHAT_UPLOAD_URL_MISSING'); er.status=502; er.data=out; throw er; }
      return {url,name:String(file?.name||name),contentType:String(file?.content_type||file?.mime_type||contentType),size:Number(file?.size||data.length),raw:out};
    }finally{clearTimeout(timer);}
  }

  async sendFile(chatId, file={}) {
    const id=String(chatId||'').trim();
    const url=String(file?.url||'').trim();
    if(!id){ const er=new Error('LIVECHAT_CHAT_ID_REQUIRED'); er.status=400; throw er; }
    if(!url){ const er=new Error('LIVECHAT_FILE_URL_REQUIRED'); er.status=400; throw er; }
    // Agent Chat API v3.6 File event request accepts type/url/visibility (plus optional
    // custom_id/properties/alternative_text). Response-only metadata such as name,
    // content_type and size must not be sent back as request fields.
    const event={type:'file',url,visibility:'all'};
    const alternativeText=String(file?.alternativeText||file?.alternative_text||'').trim();
    if(alternativeText) event.alternative_text=alternativeText.slice(0,1000);
    return this.call('send_event',{chat_id:id,event},{retries:0});
  }

  async uploadAndSendFile(chatId, bytes, meta={}) {
    const uploaded=await this.uploadFile(chatId,bytes,meta);
    const sent=await this.sendFile(chatId,uploaded);
    return {uploaded,sent};
  }
  chatActiveFlag(chat={}) {
    const th=chat?.last_thread || chat?.last_thread_summary || (Array.isArray(chat?.threads)?chat.threads.at(-1):null) || {};
    if(typeof th?.active==='boolean') return th.active;
    if(typeof chat?.active==='boolean') return chat.active;
    const status=String(chat?.status||chat?.routing_status||th?.routing_status||'').toLowerCase();
    if(['closed','archived','inactive'].includes(status)) return false;
    if(status==='active') return true;
    return null;
  }
  async endChat(chatId) {
    const id=String(chatId||'').trim();
    if(!id){ const er=new Error('LIVECHAT_CHAT_ID_REQUIRED'); er.status=400; throw er; }
    // Agent Chat API v3.6 requires `id`. `ignore_requester_presence:true` is intentional:
    // this service may close a chat even when the API requester is not currently in chat.users.
    try{
      const response=await this.call('deactivate_chat',{id,ignore_requester_presence:true});
      return {ok:true,requestShape:'id',response};
    }catch(e){
      if(Number(e?.status)===404 || /chat is inactive|inactive/i.test(String(e?.message||''))){
        return {ok:true,alreadyClosed:true,requestShape:'id'};
      }
      const er=new Error(`LIVECHAT_END_FAILED: ${String(e?.message||'unable to deactivate chat')}`);
      er.status=Number(e?.status)||502; er.cause=e; throw er;
    }
  }
  async prepareImageAttachments(attachments=[]) {
    const out=[];
    for(const a of (attachments||[]).filter(x=>x?.isImage).slice(0,3)) {
      const item={...a};
      const url=String(item.url||'');
      if(!/^https:\/\//i.test(url)) { out.push(item); continue; }
      try {
        const ctrl=new AbortController(); const timer=setTimeout(()=>ctrl.abort(),8000);
        const r=await fetch(url,{signal:ctrl.signal,redirect:'follow'}); clearTimeout(timer);
        if(!r.ok) throw new Error(`HTTP_${r.status}`);
        const ct=String(r.headers.get('content-type')||item.mime||'image/jpeg').split(';')[0];
        if(!ct.startsWith('image/')) throw new Error('NOT_IMAGE');
        const ab=await r.arrayBuffer();
        if(ab.byteLength>5*1024*1024) throw new Error('IMAGE_TOO_LARGE');
        item.url=`data:${ct};base64,${Buffer.from(ab).toString('base64')}`; item.mime=ct; item.prepared=true;
      } catch { item.prepared=false; }
      out.push(item);
    }
    return out;
  }
  async test() {
    const started = Date.now();
    const data = await this.listChats();
    const items = data?._normalizedChats || [];
    return {
      ok:true,
      connected:true,
      latencyMs:Date.now()-started,
      count:this.filterInbox(items).length,
      rawCount:items.length,
      myActiveCount:this.filterInbox(items).length,
      listSource:data?._listSource || 'none',
      foundChats:Number(data?.found_chats ?? items.length),
      hasNextPage:Boolean(data?.next_page_id),
      sampleChatIds:this.filterInbox(items).slice(0,5).map(x=>String(x?.id||'')).filter(Boolean),
      sampleStates:items.slice(0,10).map(x=>({id:String(x?.id||''),...this.chatState(x)}))
    };
  }
}

export function extractChatEvents(chat) {
  const out = [];
  const seen = new Set();
  const eventGroups = [];
  const visited = new Set();

  function walk(node, owner=null, depth=0) {
    if (!node || typeof node !== 'object' || depth > 8) return;
    if (visited.has(node)) return;
    visited.add(node);
    if (Array.isArray(node.events)) eventGroups.push({ owner: node, events: node.events });
    if (Array.isArray(node)) { for (const x of node) walk(x, owner, depth+1); return; }
    for (const [k,v] of Object.entries(node)) { if (k !== 'events' && v && typeof v === 'object') walk(v, node, depth+1); }
  }
  walk(chat);

  function collectAttachments(ev) {
    const arr=[]; const seenUrl=new Set();
    const candidates=[];
    if(Array.isArray(ev?.attachments)) candidates.push(...ev.attachments);
    if(Array.isArray(ev?.files)) candidates.push(...ev.files);
    if(ev?.file && typeof ev.file==='object') candidates.push(ev.file);
    if(ev?.image && typeof ev.image==='object') candidates.push(ev.image);
    if(ev?.content && typeof ev.content==='object') {
      if(Array.isArray(ev.content.attachments)) candidates.push(...ev.content.attachments);
      if(ev.content.file) candidates.push(ev.content.file);
      if(ev.content.image) candidates.push(ev.content.image);
    }
    const type=String(ev?.type||ev?.event_type||'').toLowerCase();
    if(['file','image'].includes(type)) candidates.push(ev);
    for(const a of candidates){
      if(!a || typeof a!=='object') continue;
      const url=a.url||a.image_url||a.file_url||a.download_url||a.secure_url||a.src||a?.content?.url||null;
      if(!url || seenUrl.has(url)) continue; seenUrl.add(url);
      const mime=String(a.content_type||a.mime_type||a.mime||a.type||'').toLowerCase();
      const name=String(a.name||a.file_name||a.filename||'');
      const isImage=mime.startsWith('image/') || /\.(png|jpe?g|webp|gif)(?:\?|$)/i.test(String(url)) || /\.(png|jpe?g|webp|gif)$/i.test(name) || type==='image';
      arr.push({url:String(url),mime,name,isImage});
    }
    return arr.slice(0,8);
  }

  for (const {owner,events} of eventGroups) {
    for (const ev of events) {
      const type = String(ev?.type || ev?.event_type || ev?.event?.type || '').toLowerCase();
      let text = ev?.text;
      if (!text && typeof ev?.content?.text === 'string') text = ev.content.text;
      if (!text && typeof ev?.message?.text === 'string') text = ev.message.text;
      if (!text && typeof ev?.event?.text === 'string') text = ev.event.text;
      if (!text && Array.isArray(ev?.elements)) text = ev.elements.map(x=>x?.title||x?.text||x?.subtitle||'').filter(Boolean).join(' ');
      const attachments=collectAttachments(ev);
      text = String(text || '').trim();
      if (!text && attachments.length) text = attachments.some(a=>a.isImage) ? '[Member mengirim gambar]' : '[Member mengirim file]';
      if (!text) continue;
      if (type && !['message','rich_message','file','image'].includes(type) && !attachments.length) continue;
      const createdAt = ev?.created_at || owner?.created_at || new Date().toISOString();
      const ownerId = owner?.id ?? owner?.thread_id ?? '';
      const eventId = String(ev?.id ?? `${ownerId}:${createdAt}:${text}:${attachments.map(a=>a.url).join('|')}`);
      if (seen.has(eventId)) continue; seen.add(eventId);
      out.push({
        eventId, threadId:String(ownerId), createdAt, text, attachments,
        authorId: ev?.author_id || ev?.author?.id || ev?.user_id || '',
        authorType: ev?.author_type || ev?.author?.type || '', recipients: ev?.recipients || 'all'
      });
    }
  }
  return out.sort((a,b)=>String(a.createdAt).localeCompare(String(b.createdAt)));
}

