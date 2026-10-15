package lab;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;

/** All arrivals are forwarded, including duplicates, with an explicit boolean flag. */
public class TaggingDemo {
    public static final String WARMUP = "__warmup__";
    public static final String WARMUP_JSON = "{\"event_id\":\"__warmup__\",\"control\":true}";
    static final ObjectMapper JSON = new ObjectMapper();
    public static final long TTL_MS = 3_600_000L;

    static boolean duplicate(Long lastSeen, long now) {
        return lastSeen != null && now-lastSeen < TTL_MS;
    }

    public static String id(String input) {
        try { return JSON.readTree(input).get("event_id").asText(); }
        catch (IOException error) { throw new IllegalArgumentException("Invalid JSON event", error); }
    }

    public static String mark(String input, boolean duplicate) {
        try {
            ObjectNode value = (ObjectNode) JSON.readTree(input);
            value.put("is_duplicate", duplicate);
            value.put("marked_at_ms", System.currentTimeMillis());
            return value.toString();
        } catch (IOException error) { throw new IllegalArgumentException("Invalid JSON event", error); }
    }

    static Properties consumer(String isolation) {
        Properties p = new Properties();
        p.put("bootstrap.servers", Lab.bootstrap());
        p.put("group.id", "tag-inspector-" + UUID.randomUUID());
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("isolation.level", isolation);
        p.put("fetch.max.wait.ms", "10");
        return p;
    }

    static Map<String, Object> row(ConsumerRecord<String, String> r, String isolation) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("isolation", isolation);
        row.put("key", r.key()); row.put("value", r.value()); row.put("offset", r.offset());
        row.put("observed_at_ms", System.currentTimeMillis());
        return row;
    }

    static void read(String topic, String isolation) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumer(isolation))) {
            TopicPartition tp = new TopicPartition(topic, 0);
            c.assign(Collections.singleton(tp)); c.seekToBeginning(Collections.singleton(tp));
            long end = c.endOffsets(Collections.singleton(tp)).get(tp);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (c.position(tp) < end) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("Consumer did not reach log end");
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(20))) rows.add(row(r, isolation));
            }
        }
        System.out.println(Lab.json(rows));
    }

    static void observe(String topic, Path dir) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        for (String isolation : Arrays.asList("read_uncommitted", "read_committed")) {
            Thread thread = new Thread(() -> {
                try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumer(isolation));
                     BufferedWriter observations = Files.newBufferedWriter(dir.resolve("observations-"+isolation+".jsonl"));
                     BufferedWriter sink = Files.newBufferedWriter(dir.resolve("sink-"+isolation+".jsonl"))) {
                    TopicPartition tp = new TopicPartition(topic, 0);
                    c.assign(Collections.singleton(tp)); c.seek(tp, 0); c.position(tp);
                    Lab.write(dir.resolve("observer-ready-" + isolation), "true");
                    long deadline = System.nanoTime() + Duration.ofMinutes(20).toNanos();
                    int count = 0;
                    while (!Files.exists(dir.resolve("observer-stop"))) {
                        if (System.nanoTime() > deadline) throw new IllegalStateException("Observer timed out");
                        ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(10));
                        for (ConsumerRecord<String, String> r : records) {
                            observations.write(Lab.json(row(r, isolation))); observations.newLine();
                            sink.write(r.value()); sink.newLine(); count++;
                        }
                        if (!records.isEmpty()) {
                            observations.flush(); sink.flush();
                            Path temporary = dir.resolve("progress-"+isolation+".tmp");
                            Files.writeString(temporary, Integer.toString(count));
                            Files.move(temporary, dir.resolve("progress-"+isolation), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                        }
                    }
                } catch (Throwable error) { errors.add(error); }
            });
            threads.add(thread); thread.start();
        }
        for (Thread thread : threads) thread.join();
        if (!errors.isEmpty()) throw new RuntimeException("Observer failed", errors.get(0));
    }

    static class Marker implements Processor<String, String, String, String> {
        final String directory;
        ProcessorContext<String, String> context;
        KeyValueStore<String, Long> seen;
        Marker(String directory) { this.directory = directory; }
        public void init(ProcessorContext<String, String> context) {
            this.context = context; seen = context.getStateStore("seen-ids");
            try { Lab.write(Paths.get(directory,"ttl-config.json"), "{\"ttl_ms\":"+TTL_MS+",\"refresh\":\"every_arrival\",\"time_basis\":\"processing\"}"); }
            catch (Exception error) { throw new RuntimeException(error); }
        }
        public void process(org.apache.kafka.streams.processor.api.Record<String, String> record) {
            if (record.value().equals(WARMUP_JSON)) {
                try { Lab.write(Paths.get(directory, "worker-ready"), "warmup processed"); }
                catch (Exception e) { throw new RuntimeException(e); }
                return;
            }
            String eventId = id(record.value());
            if (!eventId.equals(record.key())) throw new IllegalArgumentException("Input must be keyed by event_id");
            long now = System.currentTimeMillis();
            boolean duplicate = duplicate(seen.get(eventId), now);
            seen.put(eventId, now);
            context.forward(record.withValue(mark(record.value(), duplicate)));
        }
    }

    static void streams(String input, String output, Path dir) throws Exception {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, output + "-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Lab.bootstrap());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        p.put(StreamsConfig.STATE_DIR_CONFIG, dir.resolve("state").toString());
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        p.put("topic.min.insync.replicas", "2");
        p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        p.put("producer.acks", "all"); p.put("producer.linger.ms", "0");
        p.put("producer.transaction.timeout.ms", "600000");
        p.put("consumer.auto.offset.reset", "earliest"); p.put("consumer.fetch.max.wait.ms", "10");
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("seen-ids"), Serdes.String(), Serdes.Long()));
        builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
            .process(() -> new Marker(dir.toString()), "seen-ids")
            .to(output, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams application = new KafkaStreams(builder.build(), p);
        application.setUncaughtExceptionHandler(error -> {
            try { Lab.write(dir.resolve("worker-error"), error.toString()); } catch (Exception ignored) { }
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> application.close(Duration.ofSeconds(10))));
        application.start();
        new CountDownLatch(1).await();
    }

    static void seed(String topic, int count, int rate) throws Exception {
        Properties config = Lab.producerConfig(Lab.bootstrap(), true);
        config.put("linger.ms", "5"); config.put("batch.size", "65536");
        config.put("max.in.flight.requests.per.connection", "5");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            if (count == 0) { producer.send(new ProducerRecord<>(topic, WARMUP, WARMUP_JSON)).get(); return; }
            // Resolve metadata before the rate-controlled phase; timestamp each send itself.
            producer.partitionsFor(topic);
            long started = System.nanoTime();
            long wallStarted = System.currentTimeMillis();
            AtomicReference<Throwable> failed = new AtomicReference<>();
            ConcurrentMap<Long, java.util.concurrent.atomic.AtomicInteger> buckets = new ConcurrentHashMap<>();
            ExecutorService pool = Executors.newFixedThreadPool(4);
            List<Future<?>> workers = new ArrayList<>();
            for (int worker = 0; worker < 4; worker++) {
                final int number = worker;
                workers.add(pool.submit(() -> {
                    for (int seq = number; seq < count; seq += 4) {
                        long target = started + (long)(seq * (1_000_000_000.0 / rate));
                        long remaining;
                        while ((remaining = target-System.nanoTime()) > 0) LockSupport.parkNanos(remaining);
                        String key = String.format(Locale.ROOT, "event-%05d", seq / 3);
                        ObjectNode value = JSON.createObjectNode();
                        value.put("event_id", key); value.put("input_seq", seq);
                        value.putObject("payload").put("source", "synthetic").put("value", seq).put("worker", number);
                        value.put("ingested_at_ms", System.currentTimeMillis());
                        buckets.computeIfAbsent((System.nanoTime()-started)/1_000_000_000L, ignored -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
                        producer.send(new ProducerRecord<>(topic, key, value.toString()), (metadata, error) -> { if (error != null) failed.compareAndSet(null, error); });
                    }
                }));
            }
            try { for (Future<?> worker : workers) worker.get(60, TimeUnit.SECONDS); }
            finally { pool.shutdownNow(); }
            long sentAt = System.nanoTime();
            producer.flush();
            if (failed.get() != null) throw new RuntimeException("Input publish failed", failed.get());
            Map<String,Object> result = new LinkedHashMap<>();
            result.put("records", count); result.put("target_rate_per_sec", rate); result.put("producer_threads", 4);
            result.put("started_at_ms", wallStarted); result.put("send_duration_ms", (sentAt-started)/1_000_000.0);
            result.put("acknowledged_duration_ms", (System.nanoTime()-started)/1_000_000.0);
            result.put("achieved_send_rate_per_sec", count/((sentAt-started)/1_000_000_000.0));
            Map<String,Object> ordered = new TreeMap<>(); buckets.forEach((k,v) -> ordered.put(k.toString(),v.get()));
            result.put("send_buckets_per_second", ordered);
            System.out.println(Lab.json(result));
        }
    }

    static class RedisMarker implements AutoCloseable {
        final Socket socket;
        final BufferedReader reader;
        final OutputStream stream;
        RedisMarker() throws Exception {
            socket = new Socket(System.getenv().getOrDefault("REDIS_HOST", "redis"), 6379);
            socket.setSoTimeout(10000); reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            stream = new BufferedOutputStream(socket.getOutputStream());
        }
        boolean first(String key) throws Exception {
            String script = "local old=redis.call('GET',KEYS[1]);redis.call('SET',KEYS[1],'seen','PX',ARGV[1]);if old then return 0 else return 1 end";
            String[] args = {"EVAL", script, "1", key, Long.toString(TTL_MS)};
            stream.write(("*"+args.length+"\r\n").getBytes(StandardCharsets.UTF_8));
            for (String arg : args) { byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
                stream.write(("$"+bytes.length+"\r\n").getBytes(StandardCharsets.UTF_8)); stream.write(bytes); stream.write("\r\n".getBytes(StandardCharsets.UTF_8)); }
            stream.flush(); String response = reader.readLine();
            if (":1".equals(response)) return true;
            if (":0".equals(response)) return false;
            throw new IOException("Unexpected Redis response: "+response);
        }
        public void close() throws IOException { socket.close(); }
    }

    static void direct(String input, String output, String mode, Path directory) throws Exception {
        Properties config = consumer("read_committed"); config.put("group.id", output+"-source");
        Properties producerConfig = Lab.producerConfig(Lab.bootstrap(), true); producerConfig.put("linger.ms", "5");
        Map<String,Long> seen = new HashMap<>();
        Lab.write(directory.resolve("ttl-config.json"), "{\"ttl_ms\":"+TTL_MS+",\"refresh\":\"every_arrival\",\"time_basis\":\"processing\"}");
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(config);
             KafkaProducer<String,String> producer = new KafkaProducer<>(producerConfig);
             RedisMarker redis = mode.equals("redis") ? new RedisMarker() : null) {
            TopicPartition tp = new TopicPartition(input,0); consumer.assign(Collections.singleton(tp)); consumer.seek(tp,0);
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String,String> record : records) {
                    if (record.value().equals(WARMUP_JSON)) { Lab.write(directory.resolve("worker-ready"), "warmup processed"); continue; }
                    String key = id(record.value());
                    long now = System.currentTimeMillis();
                    boolean first = redis != null ? redis.first(output+":"+key) : !duplicate(seen.put(key,now), now);
                    producer.send(new ProducerRecord<>(output, key, mark(record.value(), !first)), (metadata,error) -> { if (error != null) failure.compareAndSet(null,error); });
                }
                if (!records.isEmpty()) { producer.flush(); if (failure.get()!=null) throw new RuntimeException(failure.get()); consumer.commitSync(); }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "seed": seed(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3])); break;
            case "read": read(args[1], args[2]); break;
            case "observe": observe(args[1], Paths.get(args[2])); break;
            case "streams": streams(args[1], args[2], Paths.get(args[3])); break;
            case "direct": direct(args[1], args[2], args[3], Paths.get(args[4])); break;
            case "flink": FlinkTagging.run(args); break;
            case "ttl-policy-check":
                if (duplicate(null, 0) || !duplicate(0L, TTL_MS-1) || duplicate(0L, TTL_MS)
                    || !duplicate(TTL_MS-1, 2*TTL_MS-2) || duplicate(TTL_MS-1, 2*TTL_MS-1))
                    throw new IllegalStateException("TTL boundary policy failed");
                System.out.println("{\"ttl_ms\":"+TTL_MS+",\"boundary_policy_pass\":true}"); break;
            default: throw new IllegalArgumentException(args[0]);
        }
    }
}
