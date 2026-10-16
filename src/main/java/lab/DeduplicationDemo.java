package lab;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.*;

/** Filter duplicate IDs, rather than forwarding them with a flag. */
public class DeduplicationDemo {
    static class Filter extends KeyedProcessFunction<String,String,Tuple2<String,String>>
            implements CheckpointedFunction, CheckpointListener {
        final String directory, mode, fault;
        transient ValueState<Boolean> seen;
        transient int attempt;
        int processed;
        Filter(String directory,String mode,String fault) {
            this.directory=directory;this.mode=mode;this.fault=fault;
        }
        public void initializeState(FunctionInitializationContext context)throws Exception {
            attempt=getRuntimeContext().getAttemptNumber();
            Lab.write(Paths.get(directory,"attempt-"+attempt+"-initialized.json"),
                "{\"attempt\":"+attempt+",\"is_restored\":"+context.isRestored()
                +",\"at_ms\":"+System.currentTimeMillis()+"}");
        }
        public void open(OpenContext context)throws Exception {
            ValueStateDescriptor<Boolean> descriptor=new ValueStateDescriptor<>("admitted-ids",Types.BOOLEAN);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMillis(TaggingDemo.TTL_MS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());
            seen=getRuntimeContext().getState(descriptor);
        }
        public void processElement(String input,Context context,Collector<Tuple2<String,String>> out)throws Exception {
            if(input.equals(TaggingDemo.WARMUP_JSON)) {
                Lab.write(Paths.get(directory,"worker-ready"),"warmup processed");return;
            }
            boolean duplicate=false;
            if(mode.startsWith("state-")) {
                duplicate=Boolean.TRUE.equals(seen.value());seen.update(true);
            }
            if(!duplicate)out.collect(Tuple2.of(TaggingDemo.id(input),input));
            processed++;
        }
        public void snapshotState(FunctionSnapshotContext context)throws Exception {
            Path witness=Paths.get(directory,"checkpoint-fault.json");
            if(fault.equals("checkpoint")&&processed>0&&!Files.exists(witness)) {
                Lab.write(witness,"{\"checkpoint_id\":"+context.getCheckpointId()+",\"attempt\":"+attempt
                    +",\"processed_before_failure\":"+processed+",\"at_ms\":"+System.currentTimeMillis()+"}");
                throw new IOException("INJECTED_DEDUPLICATION_CHECKPOINT_FAILURE");
            }
        }
        public void notifyCheckpointComplete(long id)throws Exception {
            Lab.write(Paths.get(directory,"completed-"+id+"-attempt-"+attempt+".json"),
                "{\"checkpoint_id\":"+id+",\"attempt\":"+attempt+",\"processed\":"+processed
                +",\"completed_at_ms\":"+System.currentTimeMillis()+"}");
        }
        public void notifyCheckpointAborted(long id)throws Exception {
            Lab.write(Paths.get(directory,"aborted-"+id+"-attempt-"+attempt+".json"),
                "{\"checkpoint_id\":"+id+",\"attempt\":"+attempt+"}");
        }
    }

    /** Persistent RESP connection. Only bulk ASCII ledger values are used. */
    static class RedisConnection implements AutoCloseable {
        final Socket socket;
        final BufferedReader input;
        final OutputStream output;
        RedisConnection()throws IOException {
            socket=new Socket(System.getenv().getOrDefault("REDIS_HOST","redis"),
                Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT","6379")));
            socket.setSoTimeout(5000);socket.setTcpNoDelay(true);
            input=new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
            output=new BufferedOutputStream(socket.getOutputStream());
        }
        Object call(String...args)throws IOException {
            output.write(("*"+args.length+"\r\n").getBytes(StandardCharsets.UTF_8));
            for(String arg:args) {
                byte[] value=arg.getBytes(StandardCharsets.UTF_8);
                output.write(("$"+value.length+"\r\n").getBytes(StandardCharsets.UTF_8));
                output.write(value);output.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
            output.flush();String line=input.readLine();
            if(line==null)throw new EOFException("Redis disconnected");
            char type=line.charAt(0);String data=line.substring(1);
            if(type=='-')throw new IOException(data);
            if(type=='+')return data;
            if(type==':')return Long.parseLong(data);
            if(type=='$') {
                int size=Integer.parseInt(data);if(size<0)return null;
                char[] value=new char[size];int offset=0;
                while(offset<size) {int n=input.read(value,offset,size-offset);if(n<0)throw new EOFException();offset+=n;}
                if(!"".equals(input.readLine()))throw new IOException("Invalid Redis bulk terminator");
                return new String(value);
            }
            throw new IOException("Unsupported Redis reply "+type);
        }
        public void close()throws IOException {socket.close();}
    }

    /** The check, Kafka ACK and ledger confirmation are all inside invoke(). */
    static class RedisSink extends RichSinkFunction<Tuple2<String,String>> {
        final String topic,directory,mode,fault;
        transient KafkaProducer<String,String> producer;
        transient RedisConnection redis;
        RedisSink(String topic,String directory,String mode,String fault) {
            this.topic=topic;this.directory=directory;this.mode=mode;this.fault=fault;
        }
        public void open(OpenContext context)throws Exception {
            producer=new KafkaProducer<>(Lab.producerConfig(Lab.bootstrap(),true));
            redis=new RedisConnection();
        }
        void inject(String stage,Tuple2<String,String> value,RecordMetadata metadata)throws Exception {
            Path witness=Paths.get(directory,"sink-gap.json");
            if(!fault.equals(stage)||Files.exists(witness))return;
            Object stored=redis.call("GET",topic+":"+value.f0);
            String record="{\"stage\":"+Lab.json(stage)+",\"attempt\":"+getRuntimeContext().getAttemptNumber()
                +",\"event_id\":"+Lab.json(value.f0)+",\"input\":"+value.f1
                +",\"redis_value\":"+Lab.json(stored)+",\"kafka_acked\":"+(metadata!=null)
                +",\"kafka_offset\":"+(metadata==null?-1:metadata.offset())+",\"at_ms\":"+System.currentTimeMillis()+"}";
            Lab.write(witness,record);
            throw new IOException("INJECTED_SINK_GAP_"+stage);
        }
        public void invoke(Tuple2<String,String> value,Context context)throws Exception {
            String key=topic+":"+value.f0;
            if(mode.equals("redis-before")) {
                Object admitted=redis.call("SET",key,"reserved","NX","PX",Long.toString(TaggingDemo.TTL_MS));
                if(admitted==null) {redis.call("PEXPIRE",key,Long.toString(TaggingDemo.TTL_MS));return;}
                inject("after-redis",value,null);
            } else {
                if(redis.call("GET",key)!=null) {
                    redis.call("PEXPIRE",key,Long.toString(TaggingDemo.TTL_MS));return;
                }
            }
            RecordMetadata ack=producer.send(new ProducerRecord<>(topic,value.f0,value.f1)).get(35,TimeUnit.SECONDS);
            inject("after-ack",value,ack);
            if(mode.equals("redis-after"))redis.call("SET",key,"sent","PX",Long.toString(TaggingDemo.TTL_MS));
        }
        public void close()throws Exception {
            try {if(producer!=null)producer.close(Duration.ofSeconds(10));}
            finally {if(redis!=null)redis.close();}
        }
    }

    public static void main(String[] args)throws Exception {
        String input=args[0],output=args[1],mode=args[2],directory=args[3],fault=args[4];
        Configuration config=new Configuration();config.setString("rest.bind-address","127.0.0.1");
        config.setInteger("rest.port",0);
        config.setString("state.checkpoints.dir",Paths.get(directory,"checkpoints").toUri().toString());
        StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironment(1,config);
        env.setParallelism(1);env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,Duration.ofSeconds(1)));
        KafkaSource<String> source=KafkaSource.<String>builder().setBootstrapServers(Lab.bootstrap())
            .setTopics(input).setGroupId(output+"-source").setStartingOffsets(OffsetsInitializer.earliest())
            .setProperty("isolation.level","read_committed").setProperty("fetch.max.wait.ms","10")
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var records=env.fromSource(source,WatermarkStrategy.noWatermarks(),"durable JSON input").uid("source")
            .keyBy(TaggingDemo::id).process(new Filter(directory,mode,fault)).uid("deduplication");
        if(mode.startsWith("redis-")) {
            records.addSink(new RedisSink(output,directory,mode,fault)).uid("redis-kafka-sink");
        } else {
            Properties producer=Lab.producerConfig(Lab.bootstrap(),true);
            producer.remove("key.serializer");producer.remove("value.serializer");
            producer.put("transaction.timeout.ms","600000");
            KafkaRecordSerializationSchema<Tuple2<String,String>> serializer=KafkaRecordSerializationSchema
                .<Tuple2<String,String>>builder().setTopic(output)
                .setKeySerializationSchema(v->v.f0.getBytes(StandardCharsets.UTF_8))
                .setValueSerializationSchema(v->v.f1.getBytes(StandardCharsets.UTF_8)).build();
            records.sinkTo(KafkaSink.<Tuple2<String,String>>builder().setBootstrapServers(Lab.bootstrap())
                .setKafkaProducerConfig(producer).setTransactionalIdPrefix(output+"-dedup-")
                .setDeliveryGuarantee(mode.equals("state-eos")?DeliveryGuarantee.EXACTLY_ONCE:DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(serializer).build()).uid("kafka-sink");
        }
        env.execute("filter-duplicate-ids-"+mode);
    }
}
