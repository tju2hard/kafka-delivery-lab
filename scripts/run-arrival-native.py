"""Run the pinned arrival matrix on three owned native brokers, without Docker.

Requires Python 3.10.12, an extracted kafka_2.13-3.7.0 distribution and Redis 7.2.6. Build first
with scripts/build.sh. Only processes started by this invocation are stopped.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-home',type=Path,required=True)
    parser.add_argument('--redis-server',type=Path,required=True)
    parser.add_argument('--java-home',type=Path,required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    assert sys.version_info[:3]==(3,10,12),'Python 3.10.12 required by the pinned experiment'
    root=args.output_dir.resolve();root.mkdir(parents=True,exist_ok=False)
    kafka=args.kafka_home.resolve();redis=args.redis_server.resolve()
    assert (kafka/'libs/kafka_2.13-3.7.0.jar').is_file(), 'Kafka 3.7.0 required'
    redis_version=subprocess.check_output([str(redis),'--version'],text=True).strip()
    assert 'v=7.2.6 ' in redis_version,redis_version
    os.environ['JAVA_HOME']=str(args.java_home.resolve())
    os.environ['PATH']=str(args.java_home.resolve()/'bin')+os.pathsep+os.environ['PATH']
    os.environ['KAFKA_HEAP_OPTS']='-Xms128m -Xmx384m'
    sockets=[]
    for _ in range(7):
        sock=socket.socket();sock.bind(('127.0.0.1',0));sockets.append(sock)
    ports=[s.getsockname()[1] for s in sockets]
    broker_ports=ports[:3];controller_ports=ports[3:6];redis_port=ports[6]
    os.environ.update(KAFKA_BOOTSTRAP_SERVERS=','.join(f'127.0.0.1:{p}' for p in broker_ports),
                      REDIS_HOST='127.0.0.1',REDIS_PORT=str(redis_port))
    logs=[];processes=[];experiment=None

    def interrupt(signum,frame):
        raise KeyboardInterrupt(f'Interrupted by signal {signum}')

    signal.signal(signal.SIGTERM,interrupt)

    def start(command,directory):
        directory.mkdir(exist_ok=True)
        log=(directory/'server.log').open('w');logs.append(log)
        process=subprocess.Popen(list(map(str,command)),stdout=log,stderr=subprocess.STDOUT,start_new_session=True,
                                 env={**os.environ,'LOG_DIR':str(directory/'logs')})
        processes.append(process)
        return process

    try:
        cluster='MkU3OEVBNTcwNTJENDM2Qk'
        quorum=','.join(f'{i+1}@127.0.0.1:{p}' for i,p in enumerate(controller_ports))
        for i in range(3):
            directory=root/f'broker-{i+1}';directory.mkdir()
            config=directory/'server.properties'
            settings={
                'node.id':i+1,'process.roles':'broker,controller',
                'controller.quorum.voters':quorum,
                'listeners':f'PLAINTEXT://127.0.0.1:{broker_ports[i]},CONTROLLER://127.0.0.1:{controller_ports[i]}',
                'advertised.listeners':f'PLAINTEXT://127.0.0.1:{broker_ports[i]}',
                'listener.security.protocol.map':'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
                'controller.listener.names':'CONTROLLER','inter.broker.listener.name':'PLAINTEXT',
                'log.dirs':directory/'data','offsets.topic.replication.factor':3,
                'offsets.topic.num.partitions':3,'transaction.state.log.replication.factor':3,
                'transaction.state.log.min.isr':2,'transaction.state.log.num.partitions':3,
                'group.initial.rebalance.delay.ms':0,'num.partitions':1,
                'default.replication.factor':3,'min.insync.replicas':2,
            }
            config.write_text(''.join(f'{k}={v}\n' for k,v in settings.items()))
            with (directory/'format.log').open('w') as stream:
                subprocess.run([str(kafka/'bin/kafka-storage.sh'),'format','-t',cluster,'-c',str(config)],
                               check=True,stdout=stream,stderr=subprocess.STDOUT)
        for sock in sockets:sock.close()
        for i in range(3):
            directory=root/f'broker-{i+1}'
            start([kafka/'bin/kafka-server-start.sh',directory/'server.properties'],directory)
        directory=root/'redis';directory.mkdir()
        start([redis,'--bind','127.0.0.1','--port',redis_port,'--save','',
               '--appendonly','yes','--appendfsync','always','--dir',directory],directory)
        sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
        import run_lab
        run_lab.JAVA[0]=str(args.java_home.resolve()/'bin/java')
        runtime=ROOT/'harbor/arrival-marking-comparison/environment/runtime'
        run_lab.JAVA[-2]=str(runtime/'classes')+':'+str(runtime/'lib/*')
        from arrival_comparison import ArrivalExperiment
        experiment=ArrivalExperiment(root/'arrival-native')
        report=experiment.run()
        report['execution']={'kind':'native processes','redis_version':redis_version,
            'java_version':subprocess.check_output([run_lab.JAVA[0],'-version'],stderr=subprocess.STDOUT,text=True).strip(),
            'kafka_server_jar_sha256':hashlib.sha256((kafka/'libs/kafka_2.13-3.7.0.jar').read_bytes()).hexdigest(),
            'ports':ports,'owned_server_pids':[p.pid for p in processes]}
        (root/'arrival-report.json').write_text(json.dumps(report,indent=2)+'\n')
        # Fresh, independent Kafka reads and byte/marker audits while brokers still run.
        verify_env={**os.environ,'ARRIVAL_REPORT':str(root/'arrival-report.json'),'ARRIVAL_VERIFY_OUT':str(root/'verified'),
                    'PYTHONPATH':str(ROOT/'harbor/arrival-marking-comparison/environment'),
                    'LAB_JAVA':run_lab.JAVA[0],'LAB_CLASSPATH':run_lab.JAVA[-2]}
        subprocess.run([sys.executable,str(ROOT/'harbor/arrival-marking-comparison/tests/verify.py')],
                       env=verify_env,check=True)
        checks=json.loads((root/'verified/checks.json').read_text())
        assert checks['report_validation_reward']==1,checks['errors']
        print(json.dumps({'native_matrix':'passed','cases':len(report['cases']),'artifacts':str(root)}),flush=True)
    finally:
        if experiment is not None:experiment.close()
        for sock in sockets:sock.close()
        for process in processes:
            if process.poll() is None:os.killpg(process.pid,signal.SIGTERM)
        for process in processes:
            try:process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10)
        for stream in logs:stream.close()


if __name__=='__main__':main()
