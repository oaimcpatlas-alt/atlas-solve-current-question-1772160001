
const fs = require('fs');
const { MongoClient } = require('mongodb');
const tls = require('tls');
const { Duplex } = require('stream');
const WebSocket = require('ws');
const PROJECT_ID = '699c12be8df98bd863d63d70';
const CLUSTER_NAME = 'mcpatlas';
const COOKIE_HEADER = process.env.CLOUD_COOKIES || '';
const MONGO_URI = 'mongodb://ac-lxbrbla-shard-00-02.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-01.zlknsyp.mongodb.net,ac-lxbrbla-shard-00-00.zlknsyp.mongodb.net/?tls=true&authMechanism=MONGODB-X509&authSource=%24external&serverMonitoringMode=poll&maxIdleTimeMS=30000&minPoolSize=0&maxPoolSize=5&maxConnecting=6&replicaSet=atlas-pq8tl1-shard-0';
class TLSSocketProxy extends Duplex {
  constructor(options={}){ super(); this.host=options.host||options.servername; this.port=options.port||27017; this.remoteAddress=this.host; this.remotePort=this.port; this.localAddress='atlas-proxy'; this.localPort=Math.floor(Math.random()*50000)+10000; this.connected=false; this._pending=[]; const url=new URL(`wss://cloud.mongodb.com/cluster-connection/${PROJECT_ID}`); url.searchParams.set('sniHostname',this.host); url.searchParams.set('port',String(this.port)); url.searchParams.set('clusterName',CLUSTER_NAME); url.searchParams.set('version','1'); this.ws=new WebSocket(url,{headers:{'User-Agent':'Mozilla/5.0','Origin':'https://cloud.mongodb.com','Cookie':COOKIE_HEADER}}); this.ws.on('open',()=>{ const meta={port:this.port,host:this.host,clusterName:CLUSTER_NAME,ok:1}; const payload=Buffer.from(JSON.stringify(meta),'utf8'); this.ws.send(Buffer.concat([Buffer.from([1]),payload])); }); this.ws.on('message',(data)=>{ const buf=Buffer.isBuffer(data)?data:Buffer.from(data); if(!buf.length)return; const type=buf[0], rest=buf.subarray(1); if(type===1){ let msg={}; try{msg=JSON.parse(rest.toString('utf8'))}catch(e){this.destroy(e); return;} if(msg.preMessageOk===1){ this.connected=true; this.emit('connect'); this.emit('secureConnect'); while(this._pending.length){const [c,e,cb]=this._pending.shift(); this._writeNow(c,e,cb);} } else this.destroy(new Error('Unexpected proxy handshake '+rest.toString('utf8')));} else if(type===2){ this.push(rest);} else {this.destroy(new Error('Unexpected frame '+type));}}); this.ws.on('error',(e)=>this.destroy(e)); this.ws.on('close',(code,reason)=>{ if(!this.destroyed){ if(code===1000||code===4100){ this.push(null); super.destroy(); } else this.destroy(new Error(`WebSocket closed: ${code} ${reason||''}`)); } }); }
  _read(){}
  _writeNow(chunk,encoding,callback){ try{const payload=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk,encoding); this.ws.send(Buffer.concat([Buffer.from([2]),payload]),callback);}catch(e){callback(e);} }
  _write(chunk,encoding,callback){ if(!this.connected||!this.ws||this.ws.readyState!==WebSocket.OPEN){ this._pending.push([chunk,encoding,callback]); return;} this._writeNow(chunk,encoding,callback); }
  _destroy(err,cb){ while(this._pending.length){const t=this._pending.shift(); try{t[2](err||new Error('Socket destroyed'));}catch{}} try{ if(this.ws&&(this.ws.readyState===WebSocket.OPEN||this.ws.readyState===WebSocket.CONNECTING)) this.ws.close(4100, err?String(err.message||err):'Driver closed socket'); }catch{} cb(err); }
  setKeepAlive(){return this;} setNoDelay(){return this;} setTimeout(){return this;} once(event,listener){ if(event==='secureConnect'&&this.connected){ queueMicrotask(()=>listener()); return this; } return super.once(event,listener);} }
const orig = tls.connect.bind(tls); tls.connect = function(options, callback){ const host=options&&(options.host||options.servername); const port=options&&options.port; if(host==='cloud.mongodb.com'||host==='account.mongodb.com'||port===443) return orig(options, callback); const sock=new TLSSocketProxy(options||{}); if(typeof callback==='function') sock.once('secureConnect', callback); return sock; };
function norm(s){ return String(s??'').toLowerCase().replace(/[^a-z0-9]+/g,''); }
function flatten(obj,prefix='',out={}){ if(obj instanceof Date){ out[prefix||'value']=obj.toISOString(); return out;} if(Array.isArray(obj)){ out[prefix||'value']=obj; obj.slice(0,10).forEach((v,i)=>flatten(v, `${prefix}[${i}]`, out)); return out;} if(obj && typeof obj==='object'){ if(typeof obj.toHexString==='function'){ out[prefix||'value']=obj.toHexString(); return out;} for(const [k,v] of Object.entries(obj)) flatten(v, prefix?`${prefix}.${k}`:k, out); return out;} out[prefix||'value']=obj; return out; }
function simple(doc){ const out={}; for(const [k,v] of Object.entries(doc||{}).slice(0,40)){ out[k]=v instanceof Date ? v.toISOString(): (Array.isArray(v)?v.slice(0,5):v); } return out; }
function scoreKeys(keys, words){ let s=0; for(const k of keys){ const nk=norm(k); for(const [w,val] of words){ if(nk.includes(w)) s += val; } } return s; }
(async()=>{
  const result={started_at:new Date().toISOString(), cookieHeaderLength: COOKIE_HEADER.length};
  if(!COOKIE_HEADER){ result.error='no cloud cookies'; console.log(JSON.stringify(result,null,2)); return; }
  const client=new MongoClient(MONGO_URI,{serverSelectionTimeoutMS:60000,connectTimeoutMS:60000,socketTimeoutMS:60000});
  try{
    await client.connect();
    result.ping=await client.db('admin').command({ping:1});
    const dbs=(await client.db('admin').admin().listDatabases()).databases.map(d=>d.name).filter(n=>!['admin','local','config'].includes(n));
    result.databases=dbs;
    const collectionFindings=[];
    for(const dbName of dbs){
      const cols=await client.db(dbName).listCollections().toArray();
      for(const meta of cols){
        const collName=meta.name;
        const coll=client.db(dbName).collection(collName);
        const samples=await coll.find({}).limit(30).toArray().catch(()=>[]);
        const keys=new Set();
        const values=[];
        for(const doc of samples){ const flat=flatten(doc); for(const [k,v] of Object.entries(flat)){ keys.add(k); if(typeof v==='string') values.push(v.slice(0,120)); } }
        const arr=[...keys];
        const nameText=`${dbName} ${collName}`;
        const financeScore = scoreKeys([nameText,...arr], [['profit',30],['profits',30],['revenue',25],['expense',15],['spend',10],['quarter',20],['q1',20],['netincome',25],['campaign',8],['advert',8]]);
        const employeeScore = scoreKeys([nameText,...arr], [['employee',25],['staff',20],['name',2],['satisfaction',30],['overall',10],['communication',25],['colleague',25],['coworker',25],['relationship',20],['skill',10],['review',8],['team',5]]);
        if(financeScore>0 || employeeScore>0){
          collectionFindings.push({db:dbName, collection:collName, financeScore, employeeScore, sampleKeys:arr.slice(0,120), sampleDocs:samples.slice(0,3).map(simple)});
        }
      }
    }
    collectionFindings.sort((a,b)=>(Math.max(b.financeScore,b.employeeScore)-Math.max(a.financeScore,a.employeeScore)) || ((b.financeScore+b.employeeScore)-(a.financeScore+a.employeeScore)));
    result.collectionFindings=collectionFindings.slice(0,30);

    // Try targeted query if employee-style keys are found
    const emp = collectionFindings.find(x=>x.employeeScore>=50);
    if(emp){
      const coll=client.db(emp.db).collection(emp.collection);
      const docs=await coll.find({}).limit(500).toArray();
      const analyzed=[];
      for(const doc of docs){
        const flat=flatten(doc); const entries=Object.entries(flat);
        let overall=null, rel=null, comm=null, name=null;
        for(const [k,v] of entries){ const nk=norm(k); if(name==null && /name|employee|staff/.test(nk) && (typeof v==='string'||typeof v==='number')) name=v; if(overall==null && nk.includes('overall') && nk.includes('satisfaction')) overall=v; if(rel==null && nk.includes('relationship') && nk.includes('colleague')) rel=v; if(comm==null && nk.includes('communication') && nk.includes('skill')) comm=v; }
        if(overall!=null || rel!=null || comm!=null) analyzed.push({name, overall, rel, comm, sample:simple(doc)});
      }
      result.employeeAnalyzed=analyzed.slice(0,40);
    }
    const fin = collectionFindings.find(x=>x.financeScore>=50);
    if(fin){
      const coll=client.db(fin.db).collection(fin.collection);
      const docs=await coll.find({}).limit(500).toArray();
      result.financeSamples=docs.slice(0,20).map(simple);
    }
  }catch(e){ result.error=String(e&&e.message||e); result.stack=e&&e.stack||null; }
  finally{ try{await client.close();}catch{} result.finished_at=new Date().toISOString(); console.log(JSON.stringify(result,null,2)); }
})();
