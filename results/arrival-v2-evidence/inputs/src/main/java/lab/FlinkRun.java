package lab;

import java.nio.file.*;
import java.util.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.*;

public class FlinkRun {
    static class Source extends RichParallelSourceFunction<String> implements CheckpointedFunction,CheckpointListener {
        final String directory,phase;final int count;volatile boolean running=true;int cursor;transient ListState<Integer> saved;final Map<Long,Integer> snapshots=new java.util.concurrent.ConcurrentHashMap<>();
        Source(String dir,String phase,int count){this.directory=dir;this.phase=phase;this.count=count;}
        public void initializeState(FunctionInitializationContext c)throws Exception{saved=c.getOperatorStateStore().getListState(new ListStateDescriptor<>("cursor",Types.INT));if(c.isRestored())for(int x:saved.get())cursor=x;Lab.write(Paths.get(directory).resolve(phase+"-restored"),Boolean.toString(c.isRestored()));}
        public void snapshotState(FunctionSnapshotContext c)throws Exception{
            Path d=Paths.get(directory);
            if(cursor>0&&Files.exists(d.resolve("block-checkpoint"))){Lab.write(d.resolve("checkpoint-blocked"),"true");while(running&&Files.exists(d.resolve("block-checkpoint")))Thread.sleep(10);}
            saved.clear();saved.add(cursor);snapshots.put(c.getCheckpointId(),cursor);
        }
        public void notifyCheckpointComplete(long id)throws Exception{Integer n=snapshots.get(id);if(n!=null)Lab.write(Paths.get(directory).resolve(phase+"-checkpoint-"+id+"-index-"+n),Long.toString(id));}
        public void run(SourceContext<String> c)throws Exception{
            Path d=Paths.get(directory);
            while(running&&!Files.exists(d.resolve("go")))Thread.sleep(10);
            while(running&&cursor<count){synchronized(c.getCheckpointLock()){c.collect(Lab.event(cursor));cursor++;}if(phase.equals("first"))break;}
            while(running)Thread.sleep(20);
        }
        public void cancel(){running=false;}
    }
    static class Dedup extends KeyedProcessFunction<String,String,String> {
        transient ValueState<Boolean> seen;
        public void open(OpenContext c)throws Exception{seen=getRuntimeContext().getState(new ValueStateDescriptor<>("seen",Types.BOOLEAN));}
        public void processElement(String value,Context c,Collector<String> out)throws Exception{if(!Boolean.TRUE.equals(seen.value())){seen.update(true);out.collect(value);}}
    }
    public static void run(String[] a)throws Exception {
        String topic=a[1],mode=a[2],phase=a[3],directory=a[4],restore=a[5];int n=Integer.parseInt(a[6]);
        Configuration conf=new Configuration();conf.setString("rest.bind-address","127.0.0.1");conf.setInteger("rest.port",0);conf.setString("taskmanager.memory.process.size","1024m");conf.setString("jobmanager.memory.process.size","768m");conf.setString("state.checkpoints.dir",Paths.get(directory,"checkpoints").toUri().toString());
        conf.setString("execution.checkpointing.externalized-checkpoint-retention","RETAIN_ON_CANCELLATION");if(!restore.equals("none"))conf.setString("execution.savepoint.path",restore);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironment(1,conf);env.setParallelism(1);env.enableCheckpointing(1000);env.getCheckpointConfig().setCheckpointTimeout(120000);env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        Properties sinkProperties=Lab.producerConfig(Lab.bootstrap(),true);sinkProperties.remove("key.serializer");sinkProperties.remove("value.serializer");sinkProperties.put("transaction.timeout.ms","300000");
        KafkaSink<String> sink=KafkaSink.<String>builder().setBootstrapServers(Lab.bootstrap()).setKafkaProducerConfig(sinkProperties).setTransactionalIdPrefix(topic+"-flink-").setDeliveryGuarantee(mode.equals("exactly-once")?DeliveryGuarantee.EXACTLY_ONCE:DeliveryGuarantee.AT_LEAST_ONCE).setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(topic).setKeySerializationSchema(new SimpleStringSchema()).setValueSerializationSchema(new SimpleStringSchema()).build()).build();
        env.addSource(new Source(directory,phase,n)).name("durable-input").uid("source").keyBy(Lab::id).process(new Dedup()).name("keyed-dedup").uid("dedup").sinkTo(sink).name("native-kafka-sink").uid("sink");
        env.execute("delivery-lab-"+topic);
    }
}
