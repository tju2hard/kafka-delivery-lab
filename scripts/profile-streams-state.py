"""Native diagnostic: RocksDB vs heap stores, fixed early/late windows, EOS retained.

Run inside this lab image. It reuses frozen source/observer/audit code; it does
not change the original 9000-message experiment or claim platform validation.
"""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid
sys.path.insert(0,'/app')
import arrival_comparison as comparison
from arrival_comparison import ArrivalExperiment
from latency import business_rows,distribution,jsonl
from run_lab import JAVA,wait_for

class Probe(ArrivalExperiment):
    def tag(self,args,directory):
        if args[0]!='arrival-streams':return super().tag(args,directory)
        self.serial+=1
        out=(directory/f'probe-{self.serial}.stdout').open('w');err=(directory/f'probe-{self.serial}.stderr').open('w');self.streams.extend((out,err))
        profile=directory/'recording.jfr'
        command=JAVA[:-1]+['lab.StreamsStateProbe']+list(map(str,args[1:]))+[self.backend]
        command.insert(1,f'-XX:StartFlightRecording=name=StateProbe,settings=profile,filename={profile},dumponexit=true')
        worker=subprocess.Popen(command,stdout=out,stderr=err,start_new_session=True)
        worker.lab_output,worker.lab_error=Path(out.name),Path(err.name)
        worker.lab_profile=profile;self.processes.append(worker)
        return worker
    def kill(self,worker):
        if getattr(worker,'lab_profile',None) and worker.poll() is None:
            command=['/opt/java/openjdk/bin/jcmd',str(worker.pid),'JFR.dump','name=StateProbe','filename='+str(worker.lab_profile)]
            p=subprocess.run(command,capture_output=True,text=True,timeout=30)
            assert p.returncode==0,p.stdout+p.stderr
            worker.lab_profile=None
        return super().kill(worker)
    def export(self,case):
        directory=Path(case['input_file']['path']).parent
        exports=[]
        for name in ('input','sink-read_uncommitted','sink-read_committed','observations-read_uncommitted','observations-read_committed'):
            source=directory/(name+'.jsonl');target=directory/(name+'.jsonl.gz')
            with gzip.open(target,'wb') as out:out.write(source.read_bytes())
            exports.append({'file':str(target.relative_to(self.root)),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'rows':len(jsonl(source))})
        case['exports']=exports
        rows=jsonl(directory/'observations-read_committed.jsonl')
        raw={r['offset']:r for r in jsonl(directory/'observations-read_uncommitted.jsonl')}
        start=case['generation']['started_at_ms'];windows=[]
        # Windows fixed before running, not selected based on the result.
        for begin,end in ((0,3000),(10000,20000),(20000,30000)):
            selected=[r for r in rows if begin<=json.loads(r['value'])['ingested_at_ms']-start<end]
            if not selected:continue
            metrics={key:[] for key in ('send_to_mark_ms','mark_to_raw_observed_ms','raw_to_committed_observed_ms','end_to_end_ms')}
            for r in selected:
                v=json.loads(r['value']);prior=raw[r['offset']]
                assert prior['value']==r['value']
                metrics['send_to_mark_ms'].append(v['marked_at_ms']-v['ingested_at_ms'])
                metrics['mark_to_raw_observed_ms'].append(prior['observed_at_ms']-v['marked_at_ms'])
                metrics['raw_to_committed_observed_ms'].append(r['observed_at_ms']-prior['observed_at_ms'])
                metrics['end_to_end_ms'].append(r['observed_at_ms']-v['ingested_at_ms'])
            summaries={}
            for key,values in metrics.items():
                values.sort();summaries[key]={'samples':len(values),'min':values[0],'p50':values[(len(values)-1)//2],
                    'p99':values[int((len(values)-1)*.99)],'max':values[-1],'negative_samples':sum(v<0 for v in values)}
            windows.append({'source_send_window_ms':[begin,end],'metrics':summaries})
        case['fixed_windows']=windows
        print(json.dumps({'case':case['name'],'backend':self.backend,'audit':case['views']['read_committed']['audit'],'windows':windows}),flush=True)
        return case

if __name__=='__main__':
    root=Path('/app/state-probe-runs')/('state-'+uuid.uuid4().hex[:8]);probe=Probe(root);root.mkdir(parents=True)
    try:
        wait_for(lambda:int(probe.call('ready',timeout=20).strip())==3,120,'Kafka ready')
        assert Path('/opt/java/openjdk/bin/jcmd').exists()
        results=[]
        for index,backend in enumerate(('rocksdb','memory','memory','rocksdb'),1):
            probe.backend=backend;comparison.COUNT=90000
            results.append(probe.export(probe.streams_case(f'streams-{backend}-long-{index}')))
            (root/'partial.json').write_text(json.dumps({'run_directory':str(root),'cases':results},indent=2)+'\n')
        probe.backend='memory';comparison.COUNT=9000
        results.append(probe.export(probe.streams_case('streams-memory-recovery')))
        (root/'report.json').write_text(json.dumps({'run_directory':str(root),'contract':'preserve all arrivals, correct flags, EOSv2, 1h state',
            'fixture':'90000 arrivals/30s for ABBA normal cases, 9000 arrivals/3s for ACK-before-commit crash',
            'fixed_normal_windows_ms':[[0,3000],[10000,20000],[20000,30000]],'cases':results},indent=2)+'\n')
    finally:probe.close()
