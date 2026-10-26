// Separate Worker: cloud-based Discord Gateway bridge.
// Requires Durable Object binding GATEWAY, BOT_TOKEN secret,
// RELAY_SECRET secret, START_SECRET secret, and MAIN_WORKER_URL text variable.

export class DiscordGateway {
  constructor(ctx, env) {
    this.ctx=ctx;
    this.env=env;
    this.ws=null;
    this.heartbeat=null;
    this.sequence=null;
    this.sessionId=null;
    this.resumeUrl=null;
    this.connected=false;
    this.starting=false;
  }
  async fetch(request) {
    if (new URL(request.url).pathname !== '/start') return new Response('not found',{status:404});
    if (!this.starting && (!this.ws || this.ws.readyState > 1)) await this.connect();
    return new Response(JSON.stringify({state:this.ws?.readyState ?? -1,session:!!this.sessionId}),{headers:{'content-type':'application/json'}});
  }
  async alarm() {
    if(!this.ws || this.ws.readyState > 1) await this.connect();
    await this.ctx.storage.setAlarm(Date.now()+60000);
  }
  async connect() {
    if(this.starting || !this.env.BOT_TOKEN) return;
    this.starting=true;
    try {
      const url=this.resumeUrl || 'wss://gateway.discord.gg/?v=10&encoding=json';
      const ws=new WebSocket(url);
      this.ws=ws;
      ws.addEventListener('message',ev=>this.ctx.waitUntil(this.onMessage(ev.data)));
      ws.addEventListener('close',ev=>{
        this.connected=false;
        this.clearHeartbeat();
        this.ws=null;
        if([4004,4010,4011,4013,4014].includes(ev.code)){
          console.error('Discord Gateway configuration error',ev.code);
          return;
        }
        this.ctx.waitUntil(this.ctx.storage.setAlarm(Date.now()+15000));
      });
      ws.addEventListener('error',()=>console.error('Discord Gateway socket error'));
      await this.ctx.storage.setAlarm(Date.now()+60000);
    }catch(e){
      console.error('Gateway connection failed',e);
      await this.ctx.storage.setAlarm(Date.now()+15000);
    }finally{this.starting=false;}
  }
  clearHeartbeat(){if(this.heartbeat!==null){clearInterval(this.heartbeat);this.heartbeat=null;}}
  send(payload){if(this.ws && this.ws.readyState===1)this.ws.send(JSON.stringify(payload));}
  async onMessage(raw) {
    let packet;
    try{packet=JSON.parse(raw);}catch{return;}
    if(packet.s!==null && packet.s!==undefined)this.sequence=packet.s;
    if(packet.op===10){
      this.clearHeartbeat();
      const ms=Math.max(1000,packet.d?.heartbeat_interval||45000);
      this.heartbeat=setInterval(()=>this.send({op:1,d:this.sequence}),ms);
      this.send({op:1,d:this.sequence});
      if(this.sessionId && this.sequence!==null){
        this.send({op:6,d:{token:this.env.BOT_TOKEN,session_id:this.sessionId,seq:this.sequence}});
      }else{
        // GUILDS + GUILD_MESSAGES + MESSAGE_CONTENT
        this.send({op:2,d:{token:this.env.BOT_TOKEN,intents:1|512|32768,properties:{os:'linux',browser:'household-cloudflare',device:'household-cloudflare'}}});
      }
      return;
    }
    if(packet.op===7){this.ws?.close(4000,'Reconnect');return;}
    if(packet.op===9){this.sessionId=null;this.sequence=null;this.resumeUrl=null;this.ws?.close(4000,'Invalid session');return;}
    if(packet.op!==0)return;
    if(packet.t==='READY'){
      this.sessionId=packet.d?.session_id;
      this.resumeUrl=packet.d?.resume_gateway_url ? packet.d.resume_gateway_url+'/?v=10&encoding=json' : null;
      this.connected=true;
      console.log('Discord Gateway ready');
      return;
    }
    if(packet.t!=='MESSAGE_CREATE')return;
    const m=packet.d;
    if(!m || m.author?.bot || !m.guild_id || !m.content || !m.id)return;
    if(!this.env.MAIN_WORKER_URL || !this.env.RELAY_SECRET)return;
    const dest=this.env.MAIN_WORKER_URL.replace(/\/$/,'')+'/relay-message';
    try{
      const res=await fetch(dest,{
        method:'POST',headers:{'Content-Type':'application/json','x-household-relay-secret':this.env.RELAY_SECRET},
        body:JSON.stringify({id:m.id,content:m.content.slice(0,1000),channel_id:m.channel_id,guild_id:m.guild_id,author:{id:m.author.id,bot:!!m.author.bot}})
      });
      if(!res.ok)console.error('Relay returned HTTP',res.status);
    }catch(e){console.error('Relay network error',e);}
  }
}

export default {
  async fetch(request,env) {
    if(new URL(request.url).pathname!=='/start')return new Response('Gateway bridge online');
    if(!env.START_SECRET || request.headers.get('x-start-secret')!==env.START_SECRET)return new Response('Forbidden',{status:403});
    const id=env.GATEWAY.idFromName('household-discord-gateway');
    const stub=env.GATEWAY.get(id);
    return stub.fetch('https://internal/start');
  },
  async scheduled(_event,env,ctx) {
    const id=env.GATEWAY.idFromName('household-discord-gateway');
    ctx.waitUntil(env.GATEWAY.get(id).fetch('https://internal/start'));
  }
};
