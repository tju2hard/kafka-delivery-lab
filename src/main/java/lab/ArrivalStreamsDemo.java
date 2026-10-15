package lab;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;

/** All-arrivals marking with state, source offsets and output in Kafka EOSv2. */
public class ArrivalStreamsDemo {
    public static class BeforeCommitAck implements ProducerInterceptor<byte[],byte[]> {
        static Path directory;
        static String output;
        static boolean enabled;
        static final AtomicReference<String> first=new AtomicReference<>();
        static final AtomicBoolean fired=new AtomicBoolean();
        public void configure(Map<String,?> configs) { }
        public ProducerRecord<byte[],byte[]> onSend(ProducerRecord<byte[],byte[]> record) {
            if(output.equals(record.topic()))first.compareAndSet(null,new String(record.value(),StandardCharsets.UTF_8));
            return record;
        }
        public void onAcknowledgement(org.apache.kafka.clients.producer.RecordMetadata metadata,Exception error) {
            if(!enabled||error!=null||metadata==null||!output.equals(metadata.topic())
                    ||Files.exists(directory.resolve("ack-before-commit.json"))||!fired.compareAndSet(false,true))return;
            try {
                Lab.write(directory.resolve("ack-before-commit.json"),
                    "{\"pid\":"+ProcessHandle.current().pid()+",\"topic\":"+Lab.json(metadata.topic())
                    +",\"partition\":"+metadata.partition()+",\"offset\":"+metadata.offset()
                    +",\"value\":"+first.get()+",\"at_ms\":"+System.currentTimeMillis()+"}");
                // The Sender cannot send EndTxn while inside this ACK callback.
                // The controller verifies the uncommitted record, then SIGKILLs this owned JVM.
                long deadline=System.nanoTime()+Duration.ofSeconds(120).toNanos();
                while(System.nanoTime()<deadline)Thread.sleep(50);
                throw new IllegalStateException("Fault controller did not terminate the ACK-stalled JVM");
            } catch(Exception failure) {throw new RuntimeException(failure);}
        }
        public void close() { }
    }

    static class Filter implements Processor<String,String,String,String> {
        final Path directory;
        ProcessorContext<String,String> context;
        KeyValueStore<String,Long> seen;
        Filter(Path directory) {this.directory=directory;}
        public void init(ProcessorContext<String,String> context) {
            this.context=context;seen=context.getStateStore("seen-ids");
        }
        public void process(org.apache.kafka.streams.processor.api.Record<String,String> record) {
            if(record.value().equals(TaggingDemo.WARMUP_JSON)) {
                try {Lab.write(directory.resolve("worker-ready"),"warmup processed");}
                catch(Exception failure) {throw new RuntimeException(failure);}
                return;
            }
            String eventId=TaggingDemo.id(record.value());
            if(!eventId.equals(record.key()))throw new IllegalArgumentException("Kafka input must be keyed by event_id");
            long now=System.currentTimeMillis();Long last=seen.get(eventId);
            boolean duplicate=TaggingDemo.duplicate(last,now);
            seen.put(eventId,now); // Including repeats: refresh the rolling one-hour horizon.
            context.forward(record.withValue(TaggingDemo.mark(record.value(),duplicate)));
        }
    }

    public static void main(String[] args)throws Exception {
        if(args[0].equals("offset")) {
            Properties properties=new Properties();properties.put("bootstrap.servers",Lab.bootstrap());
            try(AdminClient admin=AdminClient.create(properties)) {
                var offsets=admin.listConsumerGroupOffsets(args[1]).partitionsToOffsetAndMetadata().get(15,TimeUnit.SECONDS);
                var offset=offsets.get(new TopicPartition(args[2],0));
                System.out.println(offset==null?-1:offset.offset());
            }
            return;
        }
        String input=args[0],output=args[1];Path directory=Paths.get(args[2]);boolean fault=args[3].equals("ack-crash");
        long pid=ProcessHandle.current().pid();
        BeforeCommitAck.directory=directory;BeforeCommitAck.output=output;BeforeCommitAck.enabled=fault;
        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,output+"-filter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,Lab.bootstrap());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"100");
        config.put(StreamsConfig.STATE_DIR_CONFIG,directory.resolve("state").toString());
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,"1");
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,"3");config.put("topic.min.insync.replicas","2");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
        config.put("producer.acks","all");config.put("producer.linger.ms","0");
        config.put("producer.transaction.timeout.ms","10000");
        config.put("producer.interceptor.classes",BeforeCommitAck.class.getName());
        config.put("consumer.auto.offset.reset","earliest");config.put("consumer.fetch.max.wait.ms","10");
        config.put("consumer.session.timeout.ms","10000");config.put("consumer.heartbeat.interval.ms","1000");
        Lab.write(directory.resolve("configuration.json"),"{\"application_id\":"+Lab.json(output+"-filter-app")
            +",\"commit_interval_ms\":100,\"transaction_timeout_ms\":10000,\"ttl_ms\":"+TaggingDemo.TTL_MS
            +",\"processing_guarantee\":\"exactly_once_v2\",\"input_key\":\"event_id\",\"filter_duplicates\":false}");
        StreamsBuilder builder=new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("seen-ids"),Serdes.String(),Serdes.Long()));
        builder.stream(input,Consumed.with(Serdes.String(),Serdes.String()))
            .process(()->new Filter(directory),"seen-ids").to(output,Produced.with(Serdes.String(),Serdes.String()));
        KafkaStreams application=new KafkaStreams(builder.build(),config);
        application.setStateListener((next,previous)->{
            try {Lab.write(directory.resolve("lifecycle-"+pid+"-"+next+".json"),
                "{\"pid\":"+pid+",\"previous\":"+Lab.json(previous.toString())+",\"state\":"+Lab.json(next.toString())
                +",\"at_ms\":"+System.currentTimeMillis()+"}");}
            catch(Exception failure) {throw new RuntimeException(failure);}
        });
        application.setGlobalStateRestoreListener(new StateRestoreListener() {
            public void onRestoreStart(TopicPartition partition,String store,long start,long end) {
                write("start",partition,store,"\"start_offset\":"+start+",\"end_offset\":"+end);
            }
            public void onBatchRestored(TopicPartition partition,String store,long offset,long count) { }
            public void onRestoreEnd(TopicPartition partition,String store,long restored) {
                write("end",partition,store,"\"restored_records\":"+restored);
            }
            void write(String stage,TopicPartition partition,String store,String details) {
                try {Lab.write(directory.resolve("restore-"+pid+"-"+partition.partition()+"-"+stage+".json"),
                    "{\"pid\":"+pid+",\"topic\":"+Lab.json(partition.topic())+",\"store\":"+Lab.json(store)+","+details+"}");}
                catch(Exception failure) {throw new RuntimeException(failure);}
            }
        });
        application.setUncaughtExceptionHandler(failure->{
            try {Lab.write(directory.resolve("worker-error-"+pid),failure.toString());}catch(Exception ignored) { }
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        Runtime.getRuntime().addShutdownHook(new Thread(()->application.close(Duration.ofSeconds(10))));
        application.start();new CountDownLatch(1).await();
    }
}
