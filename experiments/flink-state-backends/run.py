#!/usr/bin/env python3
"""Bounded, sequential, real Flink experiments; only manages its own subprocess.
Fixtures contain big-endian signed int64 IDs, ~30% duplicates, seed fixed.
Local files only. No network services except loopback Flink REST.
"""
import argparse, array, hashlib, json, os, pathlib, random, struct, subprocess, time, urllib.request, socket, sys
ROOT=pathlib.Path(__file__).resolve().parent
HTTP=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def dump(p,x):p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
def fixture(root,keys):
 p=root/f'ids64-v2-{keys}.bin';meta=p.with_suffix('.json')
 if p.exists() and meta.exists():
  m=json.loads(meta.read_text())
  with p.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==m['sha256']
  return p,m
 rng=random.Random(20261114);duplicates=round(keys*3/7);left_u=keys-1;left_d=duplicates;next_id=1
 h=hashlib.sha256()
 with p.open('wb',buffering=1<<20) as f:
  data=struct.pack('>q',0);f.write(data);h.update(data)
  while left_u+left_d:
   if rng.randrange(left_u+left_d)<left_d:
    k=rng.randrange(next_id);left_d-=1
   else:k=next_id;next_id+=1;left_u-=1
   mapped=(k*0x9e3779b97f4a7c15)&((1<<64)-1);mapped=mapped if mapped<(1<<63) else mapped-(1<<64)
   data=struct.pack('>q',mapped);f.write(data);h.update(data)
 m={'seed':20261114,'unique':keys,'duplicates':duplicates,'inputs':keys+duplicates,'sha256':h.hexdigest(),'encoding':'big-endian int64','id_mapping':'signed64(index * 0x9e3779b97f4a7c15) modulo 2^64'};dump(meta,m);return p,m

def ids(p):
 with p.open('rb') as f:
  while b:=f.read(8*131072):
   if len(b)%8:raise ValueError('Truncated int64 file')
   for (v,) in struct.iter_unpack('>q',b):yield v

def oracle(run,input_path,meta):
 # Oracle reads every input independently of Flink metrics and derives expected unique set.
 expected=bytearray(meta['unique']);first_seq=array.array('q',[-1])*meta['unique'];input_count=duplicates=0
 inverse=pow(0x9e3779b97f4a7c15,-1,1<<64)
 def decode(key):return (key*inverse)&((1<<64)-1)
 for raw_key in ids(input_path):
  key=decode(raw_key)
  assert 0<=key<len(expected)
  if not expected[key]:first_seq[key]=input_count
  input_count+=1;duplicates+=bool(expected[key]);expected[key]=1
 actual=bytearray(len(expected));out_count=repeat=unexpected=wrong_first=0;h=hashlib.sha256()
 for p in sorted(run.glob('output-*.bin')):
  it=iter(ids(p))
  for raw_key in it:
   seq=next(it);key=decode(raw_key);out_count+=1
   if not 0<=key<len(actual):unexpected+=1;continue
   repeat+=bool(actual[key]);wrong_first+=seq!=first_seq[key];actual[key]=1
 # Canonical ID set hash independent of partition/output order.
 for i,v in enumerate(actual):
  if v:h.update(struct.pack('>q',i))
 ops=[json.loads(p.read_text()) for p in run.glob('operator-*.json')]
 missing=sum(e and not a for e,a in zip(expected,actual))
 result={'input_count':input_count,'input_unique':sum(expected),'input_duplicates':duplicates,'output_count':out_count,'output_duplicate_ids':repeat,'unexpected_ids':unexpected,'missing_ids':missing,'wrong_first_arrival_seq':wrong_first,'output_canonical_sha256':h.hexdigest(),'operator_processed':sum(x['processed'] for x in ops),'operator_unique':sum(x['unique'] for x in ops),'operator_duplicates':sum(x['duplicates'] for x in ops)}
 result['passed']=(out_count==sum(expected) and repeat==unexpected==missing==wrong_first==0 and result['operator_processed']==input_count and result['operator_unique']==sum(expected) and result['operator_duplicates']==duplicates)
 dump(run/'verification.json',result);return result

def proc_sample(pid):
 try:
  status={k:v.strip() for k,v in (line.split(':',1) for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in line)}
  io={k:int(v) for k,v in (l.split(':',1) for l in pathlib.Path(f'/proc/{pid}/io').read_text().splitlines())}
  return {'at_ms':int(time.time()*1000),'rss_kb':int(status.get('VmRSS','0 kB').split()[0]),'peak_rss_kb':int(status.get('VmHWM','0 kB').split()[0]),'threads':int(status['Threads']),'io':io}
 except (FileNotFoundError,ProcessLookupError):return {}
def disk_size(p):
 total=0
 for f in p.rglob('*'):
  try:
   if f.is_file():total+=f.stat().st_size
  except FileNotFoundError:pass
 return total

def run_one(args,backend,keys,label):
 inp,meta=fixture(args.output/'fixtures',keys);run=args.output/label;run.mkdir()
 sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
 cp=str(ROOT/'target/classes')+':'+str(ROOT/'target/dependency/*')
 cmd=[args.java,'-Xms512m','-Xmx2g','-XX:MaxDirectMemorySize=1g','-XX:ActiveProcessorCount=2','--add-opens=java.base/java.lang=ALL-UNNAMED','--add-opens=java.base/java.util=ALL-UNNAMED','--add-opens=java.base/java.io=ALL-UNNAMED','--add-opens=java.base/java.nio=ALL-UNNAMED','--add-opens=java.base/sun.nio.ch=ALL-UNNAMED','-Dorg.slf4j.simpleLogger.defaultLogLevel=warn','-cp',cp,'lab.StateBackendBench',backend,str(run),str(inp),'2',str(port)]
 cpus=sorted(os.sched_getaffinity(0))[:2]
 def limit():os.sched_setaffinity(0,cpus)
 dump(run/'config.json',{'backend':backend,'parallelism':2,'input':meta,'cpu_affinity':cpus,'command':cmd,'checkpoint_interval_ms':5000,'checkpoint_storage':'local filesystem, NOT HDFS/S3','ttl_hours':6,'refresh':'every arrival','java_version':subprocess.run([args.java,'-version'],capture_output=True,text=True).stderr})
 log=(run/'java.log').open('w');p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,preexec_fn=limit)
 base=f'http://127.0.0.1:{port}';jid=None;vertices=[];metric_names={};seen_ck=set();drained_at=None;final_id=None;gc_at=None;started=False;last_disk=0;t0=time.monotonic();error=None
 def get(path):
  with HTTP.open(base+path,timeout=4) as r:return json.load(r)
 raw=(run/'samples.jsonl').open('w')
 try:
  while p.poll() is None:
   if time.monotonic()-t0>args.timeout:raise TimeoutError('bounded run timeout')
   sample=proc_sample(p.pid)
   if time.monotonic()-last_disk>5:
    sample['rocksdb_bytes']=disk_size(run/'rocksdb');sample['checkpoint_directory_bytes']=disk_size(run/'checkpoints');last_disk=time.monotonic()
   try:
    if not jid:
     jobs=get('/jobs/overview')['jobs']
     if jobs:
      jid=jobs[0]['jid'];dump(run/'job-initial.json',get('/jobs/'+jid));vertices=get('/jobs/'+jid)['vertices']
    if jid:
     if not vertices:
      vertices=get('/jobs/'+jid)['vertices'];dump(run/'job-running.json',get('/jobs/'+jid))
     ck=get('/jobs/'+jid+'/checkpoints');dump(run/'checkpoints-latest.json',ck)
     for c in ck['history']:
      if c['status']=='COMPLETED' and c['id'] not in seen_ck:
       detail=get(f'/jobs/{jid}/checkpoints/details/{c["id"]}');dump(run/f'checkpoint-{c["id"]}.json',detail);seen_ck.add(c['id'])
       for v in vertices:
        try:dump(run/f'checkpoint-{c["id"]}-{v["id"]}.json',get(f'/jobs/{jid}/checkpoints/details/{c["id"]}/subtasks/{v["id"]}'))
        except Exception:pass
     sample['vertices']=[];processed=0
     for v in vertices:
      for task in range(v['parallelism']):
       path=f'/jobs/{jid}/vertices/{v["id"]}/subtasks/{task}/metrics'
       if True:
        metric_names[path]=[m['id'] for m in get(path) if any(term in m['id'] for term in ['bench','busyTimeMsPerSecond','backPressuredTimeMsPerSecond','idleTimeMsPerSecond'])]
       if metric_names[path]:
        metrics=get(path+'?get='+','.join(metric_names[path]));sample['vertices'].append({'name':v['name'],'subtask':task,'metrics':metrics})
        processed+=sum(int(float(m['value'])) for m in metrics if m['id'].endswith('benchProcessed'))
     sample['processed']=processed;sample['completed_checkpoints']=len(seen_ck)
     if not started and (run/'baseline-memory.json').exists():
      (run/'run.start').touch();started=True;sample['run_started']=True
     if started and processed==meta['inputs'] and (run/'source.json').exists() and drained_at is None:
      drained_at=time.monotonic();final_id=max(seen_ck,default=0);sample['drained']=True
     if drained_at is not None and len([i for i in seen_ck if i>final_id])>=3 and gc_at is None:
      (run/'gc.request').touch();gc_at=time.monotonic();sample['gc_requested']=True
     if gc_at is not None and (run/'retained-memory.json').exists() and time.monotonic()-gc_at>2:
      (run/'finish').touch()
   except Exception as e:sample['poll_error']=str(e)
   raw.write(json.dumps(sample)+'\n');raw.flush();time.sleep(1)
 except BaseException as e:error=repr(e);p.terminate()
 finally:
  try:p.wait(timeout=15)
  except subprocess.TimeoutExpired:p.kill();p.wait()
  raw.close();log.close()
 status={'returncode':p.returncode,'seconds':time.monotonic()-t0,'error':error};dump(run/'status.json',status)
 if p.returncode==0 and not error:
  status['verification']=oracle(run,inp,meta)
 print(label,json.dumps(status),flush=True)
 return status

if __name__=='__main__':
 a=argparse.ArgumentParser();a.add_argument('--output',type=pathlib.Path,required=True);a.add_argument('--keys',type=int,default=250000);a.add_argument('--backends',default='heap,rocks-full,rocks-incremental');a.add_argument('--label',default='pilot');a.add_argument('--java',default='/usr/lib/jvm/java-21-openjdk-amd64/bin/java');a.add_argument('--timeout',type=int,default=900)
 args=a.parse_args();args.output=args.output.resolve();(args.output/'fixtures').mkdir(parents=True,exist_ok=True)
 results=[run_one(args,b,args.keys,args.label+'-'+b) for b in args.backends.split(',')]
 sys.exit(0 if all(r.get('verification',{}).get('passed') for r in results) else 1)
