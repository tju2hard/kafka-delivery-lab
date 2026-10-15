package lab;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public class Lab {
    public static String bootstrap() { return System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092"); }
    public static Properties producerConfig(String server, boolean idempotent) {
        Properties p=new Properties();p.put("bootstrap.servers",server);
        p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all");p.put("enable.idempotence",Boolean.toString(idempotent));p.put("retries","20");
        p.put("max.in.flight.requests.per.connection","1");p.put("linger.ms","0");
        p.put("request.timeout.ms","3000");p.put("delivery.timeout.ms","30000");p.put("max.block.ms","30000");
        return p;
    }
    public static String event(int n) { return String.format(Locale.ROOT,"event-%04d|synthetic-payload-%04d",n,n); }
    public static String id(String value) { return value.split("\\|",2)[0]; }
    public static String json(Object value) {
        if(value==null)return "null";
        if(value instanceof Number || value instanceof Boolean)return value.toString();
        if(value instanceof Map) {List<String> parts=new ArrayList<>();((Map<?,?>)value).forEach((k,v)->parts.add(json(k.toString())+":"+json(v)));return "{"+String.join(",",parts)+"}";}
        if(value instanceof Iterable) {List<String> parts=new ArrayList<>();for(Object x:(Iterable<?>)value)parts.add(json(x));return "["+String.join(",",parts)+"]";}
        return "\""+value.toString().replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r")+"\"";
    }
    public static void write(Path file,String value) throws Exception {
        Files.createDirectories(file.getParent());
        try(FileOutputStream out=new FileOutputStream(file.toFile())){out.write(value.getBytes(StandardCharsets.UTF_8));out.getFD().sync();}
    }
    static void pause(Path dir,String requested,String stage) throws Exception {
        if(!requested.equals(stage))return;
        write(dir.resolve("witness-"+stage),Long.toString(System.currentTimeMillis()));
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(100);
        while(!Files.exists(dir.resolve("release-"+stage))) {if(System.nanoTime()>deadline)throw new IOException("Fault controller did not release "+stage);Thread.sleep(10);}
    }
    public static List<Map<String,Object>> read(String topic,String isolation) throws Exception {
        Properties p=new Properties();p.put("bootstrap.servers",bootstrap());p.put("group.id","inspector-"+UUID.randomUUID());
        p.put("key.deserializer",StringDeserializer.class.getName());p.put("value.deserializer",StringDeserializer.class.getName());
        p.put("enable.auto.commit","false");p.put("isolation.level",isolation);p.put("request.timeout.ms","10000");
        List<Map<String,Object>> rows=new ArrayList<>();
        try(KafkaConsumer<String,String> c=new KafkaConsumer<>(p)) {
            TopicPartition partition=new TopicPartition(topic,0);c.assign(Collections.singleton(partition));c.seekToBeginning(Collections.singleton(partition));
            long end=c.endOffsets(Collections.singleton(partition),Duration.ofSeconds(10)).get(partition);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(c.position(partition)<end) {
                if(System.nanoTime()>deadline)throw new IOException("Inspector could not reach captured log end");
                for(ConsumerRecord<String,String> r:c.poll(Duration.ofMillis(100))) {
                    Map<String,Object> row=new LinkedHashMap<>();row.put("id",id(r.value()));row.put("value",r.value());row.put("key",r.key());row.put("offset",r.offset());row.put("producer_timestamp_ms",r.timestamp());row.put("observed_at_ms",System.currentTimeMillis());rows.add(row);
                }
            }
        }
        return rows;
    }
    static void observe(String topic,Path directory) throws Exception {
        Files.createDirectories(directory);List<Thread> threads=new ArrayList<>();
        final Object lock=new Object();final List<Throwable> failures=Collections.synchronizedList(new ArrayList<>());
        for(String isolation:Arrays.asList("read_uncommitted","read_committed")) {
            Thread t=new Thread(()->{
                try {
                    Properties p=new Properties();p.put("bootstrap.servers",bootstrap());p.put("group.id","observer-"+UUID.randomUUID());p.put("key.deserializer",StringDeserializer.class.getName());p.put("value.deserializer",StringDeserializer.class.getName());p.put("enable.auto.commit","false");p.put("isolation.level",isolation);
                    try(KafkaConsumer<String,String> c=new KafkaConsumer<>(p);FileOutputStream out=new FileOutputStream(directory.resolve("observations.jsonl").toFile(),true)) {
                        TopicPartition tp=new TopicPartition(topic,0);c.assign(Collections.singleton(tp));c.seek(tp,0);c.position(tp);write(directory.resolve("observer-ready-"+isolation),"ready");
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(240);
                        while(!Files.exists(directory.resolve("observer-stop"))) {
                            if(System.nanoTime()>deadline)throw new IOException("Observer lifetime exceeded");
                            for(ConsumerRecord<String,String> r:c.poll(Duration.ofMillis(20))) {
                                Map<String,Object> row=new LinkedHashMap<>();row.put("isolation",isolation);row.put("id",id(r.value()));row.put("offset",r.offset());row.put("producer_timestamp_ms",r.timestamp());row.put("observed_at_ms",System.currentTimeMillis());
                                synchronized(lock){out.write((json(row)+"\n").getBytes(StandardCharsets.UTF_8));out.flush();}
                            }
                        }
                    }
                }catch(Throwable e){failures.add(e);}
            });threads.add(t);t.start();
        }
        for(Thread t:threads)t.join();if(!failures.isEmpty())throw new IOException("Observer failed",failures.get(0));
    }
    static Object redis(String...args) throws Exception {
        try(Socket s=new Socket(System.getenv().getOrDefault("REDIS_HOST","redis"),6379)) {
            s.setSoTimeout(5000);OutputStream out=s.getOutputStream();out.write(("*"+args.length+"\r\n").getBytes(StandardCharsets.UTF_8));
            for(String a:args){byte[] b=a.getBytes(StandardCharsets.UTF_8);out.write(("$"+b.length+"\r\n").getBytes(StandardCharsets.UTF_8));out.write(b);out.write("\r\n".getBytes(StandardCharsets.UTF_8));}out.flush();
            BufferedReader r=new BufferedReader(new InputStreamReader(s.getInputStream(),StandardCharsets.UTF_8));String line=r.readLine();
            if(line==null)throw new IOException("Redis closed response");char kind=line.charAt(0);String data=line.substring(1);
            if(kind=='-')throw new IOException(data);if(kind==':')return Long.parseLong(data);if(kind=='+')return data;
            if(kind=='$'){int n=Integer.parseInt(data);if(n<0)return null;char[] b=new char[n];int at=0;while(at<n){int k=r.read(b,at,n-at);if(k<0)throw new EOFException();at+=k;}return new String(b);}
            throw new IOException("Unsupported Redis response "+kind);
        }
    }
    static boolean outbox(Path dir,int n,boolean mark) throws Exception {
        Process p=new ProcessBuilder("python3","/app/state.py",mark?"mark":"pending",dir.resolve("outbox.db").toString(),Integer.toString(n)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        String result=new String(p.getInputStream().readAllBytes(),StandardCharsets.UTF_8).trim();if(p.waitFor()!=0)throw new IOException("Outbox state operation failed");return result.equals("1");
    }
    static void forward(String[] a) throws Exception {
        String mode=a[1],topic=a[2];int n=Integer.parseInt(a[3]);Path dir=Paths.get(a[4]);String fault=a[5];String server=a.length>6?a[6]:bootstrap();
        Files.createDirectories(dir);String token=UUID.randomUUID().toString();boolean transactional=mode.equals("transaction-external");
        Properties props=producerConfig(server,!mode.equals("plain"));if(transactional){props.put("transactional.id",topic+"-external");props.put("transaction.timeout.ms","300000");}
        Set<String> seen=new HashSet<>();if(mode.equals("reconcile"))for(Map<String,Object> row:read(topic,"read_uncommitted"))seen.add(row.get("id").toString());
        Path cursor=dir.resolve("cursor");int start=Files.exists(cursor)?Integer.parseInt(Files.readString(cursor).trim()):0;
        try(KafkaProducer<String,String> p=new KafkaProducer<>(props)) {
            if(transactional)p.initTransactions();
            for(int i=start;i<n;i++) {
                String value=event(i),key=id(value),dedup=topic+":"+key;boolean send=true;
                if(mode.equals("redis-before")){send=redis("SET",dedup,"done","NX")==null?false:true;if(send)pause(dir,fault,"reserved");}
                if(mode.equals("redis-after"))send=redis("GET",dedup)==null;
                if(mode.equals("redis-lease")){send=redis("SET",dedup,token,"NX","PX","300")!=null;if(send){if(!token.equals(redis("GET",dedup)))throw new IOException("Lease lost before check");pause(dir,fault,"lease-checked");}}
                if(mode.equals("outbox"))send=outbox(dir,i,false);
                if(mode.equals("reconcile")){send=!seen.contains(key);if(send)pause(dir,fault,"checked-absent");}
                if(send) {
                    if(transactional)p.beginTransaction();
                    p.send(new ProducerRecord<>(topic,key,value)).get(35,TimeUnit.SECONDS);
                    pause(dir,fault,"after-append");
                    if(transactional){p.commitTransaction();pause(dir,fault,"after-commit");}
                    if(mode.equals("redis-after"))redis("SET",dedup,"done");
                    if(mode.equals("redis-lease"))redis("EVAL","if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('SET',KEYS[1],'done');return 1 else return 0 end","1",dedup,token);
                    if(mode.equals("outbox"))outbox(dir,i,true);
                }
                write(cursor,Integer.toString(i+1));
            }
            if(mode.equals("application-replay") || mode.equals("compacted-key"))p.send(new ProducerRecord<>(topic,id(event(0)),event(0))).get(35,TimeUnit.SECONDS);
        }
    }
    static void kafkaTransaction(String[] a) throws Exception {
        String input=a[1],topic=a[2],group=topic+"-source-group";int n=Integer.parseInt(a[3]);Path dir=Paths.get(a[4]);String fault=a[5];
        Properties producer=producerConfig(bootstrap(),true);producer.put("transactional.id",topic+"-atomic-source");producer.put("transaction.timeout.ms","300000");
        Properties consumer=new Properties();consumer.put("bootstrap.servers",bootstrap());consumer.put("group.id",group);consumer.put("key.deserializer",StringDeserializer.class.getName());consumer.put("value.deserializer",StringDeserializer.class.getName());consumer.put("enable.auto.commit","false");consumer.put("isolation.level","read_committed");
        try(KafkaProducer<String,String> p=new KafkaProducer<>(producer);KafkaConsumer<String,String> c=new KafkaConsumer<>(consumer)) {
            p.initTransactions();TopicPartition source=new TopicPartition(input,0);c.assign(Collections.singleton(source));OffsetAndMetadata committed=c.committed(source);long start=committed==null?0:committed.offset();
            if(start>=n)return;c.seek(source,start);p.beginTransaction();long next=start;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(25);
            while(next<n){if(System.nanoTime()>deadline)throw new IOException("Missing source records");for(ConsumerRecord<String,String> r:c.poll(Duration.ofMillis(100))){p.send(new ProducerRecord<>(topic,r.key(),r.value())).get(20,TimeUnit.SECONDS);next=r.offset()+1;}}
            p.sendOffsetsToTransaction(Collections.singletonMap(source,new OffsetAndMetadata(next)),new ConsumerGroupMetadata(group));p.flush();pause(dir,fault,"before-commit");p.commitTransaction();pause(dir,fault,"after-commit");
        }
    }
    public static void main(String[] a) throws Exception {
        switch(a[0]) {
            case "ready":try(AdminClient admin=AdminClient.create(Collections.singletonMap("bootstrap.servers",bootstrap()))){System.out.println(admin.describeCluster().nodes().get(10,TimeUnit.SECONDS).size());}break;
            case "create":try(AdminClient admin=AdminClient.create(Collections.singletonMap("bootstrap.servers",bootstrap()))){Map<String,String> c=new HashMap<>();c.put("min.insync.replicas","2");if(a.length>2&&a[2].equals("compact"))c.put("cleanup.policy","compact");admin.createTopics(Collections.singleton(new NewTopic(a[1],Collections.singletonMap(0,Arrays.asList(1,2,3))).configs(c))).all().get(25,TimeUnit.SECONDS);}break;
            case "seed":try(KafkaProducer<String,String> p=new KafkaProducer<>(producerConfig(bootstrap(),true))){for(int i=0;i<Integer.parseInt(a[2]);i++)p.send(new ProducerRecord<>(a[1],id(event(i)),event(i))).get(20,TimeUnit.SECONDS);}break;
            case "read":System.out.println(json(read(a[1],a[2])));break;
            case "group-offset":try(AdminClient admin=AdminClient.create(Collections.singletonMap("bootstrap.servers",bootstrap()))){Map<TopicPartition,OffsetAndMetadata> offsets=admin.listConsumerGroupOffsets(a[1]+"-source-group").partitionsToOffsetAndMetadata().get(15,TimeUnit.SECONDS);OffsetAndMetadata offset=offsets.get(new TopicPartition(a[2],0));System.out.println(offset==null?-1:offset.offset());}break;
            case "observe":observe(a[1],Paths.get(a[2]));break;
            case "forward":forward(a);break;
            case "transaction":kafkaTransaction(a);break;
            case "flink":FlinkRun.run(a);break;
            default:throw new IllegalArgumentException("Unknown action");
        }
    }
}
