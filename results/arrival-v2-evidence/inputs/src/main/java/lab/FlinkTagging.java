package lab;

import java.nio.file.*;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class FlinkTagging {
    static class Marker extends KeyedProcessFunction<String, String, Tuple2<String, String>>
            implements org.apache.flink.api.common.state.CheckpointListener {
        final String directory;
        final boolean forwardRaw;
        transient ValueState<Boolean> seen;
        int processed;
        Marker(String directory, boolean forwardRaw) { this.directory = directory; this.forwardRaw = forwardRaw; }
        public void open(OpenContext context) throws Exception {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-ids", Types.BOOLEAN);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(java.time.Duration.ofMillis(TaggingDemo.TTL_MS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());
            seen = getRuntimeContext().getState(descriptor);
            Lab.write(Paths.get(directory,"ttl-config.json"), "{\"ttl_ms\":"+descriptor.getTtlConfig().getTimeToLive().toMillis()+",\"refresh\":\"every_arrival\",\"time_basis\":\"processing\"}");
        }
        public void processElement(String input, Context context, Collector<Tuple2<String, String>> out) throws Exception {
            if (input.equals(TaggingDemo.WARMUP_JSON)) {
                Lab.write(Paths.get(directory, "worker-ready"), "warmup processed"); return;
            }
            boolean duplicate = Boolean.TRUE.equals(seen.value());
            seen.update(true);
            out.collect(Tuple2.of(TaggingDemo.id(input), forwardRaw ? input : TaggingDemo.mark(input, duplicate)));
            processed++;
        }
        public void notifyCheckpointComplete(long id) throws Exception {
            Lab.write(Paths.get(directory, "checkpoint-" + id),
                      "{\"checkpoint_id\":" + id + ",\"completed_at_ms\":" + System.currentTimeMillis()
                      + ",\"processed_at_notification\":" + processed + "}");
        }
    }

    public static void run(String[] args) throws Exception {
        String input = args[1], output = args[2], mode = args[3], directory = args[4];
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-address", "127.0.0.1"); configuration.setInteger("rest.port", 0);
        configuration.setString("state.checkpoints.dir", Paths.get(directory, "checkpoints").toUri().toString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
        env.setParallelism(1); env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(Lab.bootstrap())
            .setTopics(input).setGroupId(output + "-source").setStartingOffsets(OffsetsInitializer.earliest())
            .setProperty("isolation.level", "read_committed").setProperty("fetch.max.wait.ms", "10")
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        Properties producer = Lab.producerConfig(Lab.bootstrap(), true);
        producer.remove("key.serializer"); producer.remove("value.serializer");
        producer.put("transaction.timeout.ms", "600000");
        KafkaRecordSerializationSchema<Tuple2<String, String>> serializer = KafkaRecordSerializationSchema
            .<Tuple2<String, String>>builder().setTopic(output)
            .setKeySerializationSchema(value -> value.f0.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .setValueSerializationSchema(value -> value.f1.getBytes(java.nio.charset.StandardCharsets.UTF_8)).build();
        KafkaSink<Tuple2<String, String>> sink = KafkaSink.<Tuple2<String, String>>builder().setBootstrapServers(Lab.bootstrap())
            .setKafkaProducerConfig(producer).setTransactionalIdPrefix(output + "-flink-")
            .setDeliveryGuarantee(mode.equals("eos") ? DeliveryGuarantee.EXACTLY_ONCE : DeliveryGuarantee.AT_LEAST_ONCE)
            .setRecordSerializer(serializer).build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka input").uid("input")
            .keyBy(TaggingDemo::id).process(new Marker(directory, mode.equals("bridge"))).uid("mark")
            .sinkTo(sink).uid("output");
        env.execute("id-marking-" + mode);
    }
}
