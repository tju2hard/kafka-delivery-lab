#!/usr/bin/env python3
import json,pathlib,sys,math,statistics
root=pathlib.Path(sys.argv[1])
def read(p):return json.loads(p.read_text())
def lines(p):return [json.loads(x) for x in p.read_text().splitlines()]
def q(xs,p):
 xs=sorted(xs);return xs[min(len(xs)-1,math.ceil(len(xs)*p)-1)] if xs else None
def hist_bound(xs,fraction):
 buckets=[sum(x['latency_log2_buckets'][i] for x in xs) for i in range(32)];target=sum(buckets)*fraction;seen=0
 for i,n in enumerate(buckets):
  seen+=n
  if seen>=target:return 0 if i==0 else 2**i/1000
summary=[]
for d in sorted(root.iterdir()):
 if not (d/'config.json').exists():continue
 if not (d/'verification.json').exists() or not (d/'retained-memory.json').exists():
  print('INCOMPLETE RUN: '+d.name,file=sys.stderr);continue
 c=read(d/'config.json');v=read(d/'verification.json');ops=[read(x) for x in d.glob('operator-*.json')];sinks=[read(x) for x in d.glob('sink-*.json')];source=read(d/'source.json');samples=lines(d/'samples.jsonl');jvm=lines(d/'jvm.jsonl');base=read(d/'baseline-memory.json');retained=read(d/'retained-memory.json')
 first=source['start_ns'];last=max(x['last_ns'] for x in ops);secs=(last-first)/1e9
 # Map source monotonic timeline to wall time using run.start mtime (controller gate); approximate sample-window boundary only.
 start_ms=(d/'run.start').stat().st_mtime*1000;end_ms=start_ms+secs*1000
 active=[x for x in samples if start_ms<=x['at_ms']<=end_ms];jactive=[x for x in jvm if start_ms<=x['at_ms']<=end_ms]
 ck=[read(x) for x in d.glob('checkpoint-*.json') if x.stem.count('-')==1]
 load_ck=[x for x in ck if start_ms<=x['trigger_timestamp']<=end_ms];idle_ck=[x for x in ck if x['trigger_timestamp']>end_ms]
 def cks(xs):return [{'id':x['id'],'duration_ms':x['end_to_end_duration'],'checkpointed_bytes':x.get('checkpointed_size'),'full_bytes':x.get('state_size'),'trigger_timestamp':x['trigger_timestamp']} for x in xs]
 bp=[]
 for x in active:
  for vertex in x.get('vertices',[]):
   if 'source' in vertex['name']:
    bp.extend(float(m['value']) for m in vertex['metrics'] if 'backPressuredTimeMsPerSecond' in m['id'] and m['value']!='NaN')
 cpu=(jactive[-1]['cpu_ns']-jactive[0]['cpu_ns'])/1e9 if len(jactive)>1 else None
 sample_span=(jactive[-1]['at_ms']-jactive[0]['at_ms'])/1000 if len(jactive)>1 else None
 gc=(jactive[-1]['gc_ms']-jactive[0]['gc_ms']) if len(jactive)>1 else None
 r={'run':d.name,'backend':c['backend'],'inputs':v['input_count'],'unique':v['input_unique'],'duplicates':v['input_duplicates'],'verification_passed':v['passed'],'seconds_source_start_to_last_processed':secs,'records_per_second':v['input_count']/secs,'mean_source_to_state_ms':sum(x['latency_sum_us'] for x in ops)/v['input_count']/1000,'p99_source_to_state_ms_upper_bound':hist_bound(ops,.99),'mean_source_to_sink_callback_ms':sum(x['latency_sum_us'] for x in sinks)/v['output_count']/1000,'p99_source_to_sink_callback_ms_upper_bound':hist_bound(sinks,.99),'peak_rss_mib':max(x.get('peak_rss_kb',0) for x in samples)/1024,'retained_heap_mib':retained['heap_used']/2**20,'baseline_heap_mib':base['heap_used']/2**20,'retained_heap_delta_bytes_per_id':(retained['heap_used']-base['heap_used'])/v['input_unique'],'sampled_active_cpu_seconds':cpu,'sampled_active_wall_seconds':sample_span,'sample_coverage_ratio':sample_span/secs if sample_span else None,'sampled_average_cpu_cores':cpu/sample_span if sample_span else None,'sampled_active_gc_ms':gc,'source_backpressure_mean_ms_per_second':statistics.mean(bp) if bp else None,'peak_rocksdb_directory_mib':max(x.get('rocksdb_bytes',0) for x in samples)/2**20,'process_total_read_bytes':max(x.get('io',{}).get('read_bytes',0) for x in samples),'process_total_write_bytes':max(x.get('io',{}).get('write_bytes',0) for x in samples),'active_checkpoints':cks(load_ck),'post_load_checkpoints':cks(idle_ck),'all_checkpoints':cks(ck),'checkpoint_counts':read(d/'checkpoints-latest.json').get('counts',{})}
 summary.append(r)
print(json.dumps(summary,indent=2))
