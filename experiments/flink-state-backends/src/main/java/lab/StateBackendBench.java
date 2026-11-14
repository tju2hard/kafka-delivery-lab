package lab;

import java.nio.file.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.lang.management.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/** Numeric ID dedup, six-hour processing-time TTL. Real Flink, no synthetic state padding. */
public class StateBackendBench {
 public static class Event { public long key,sentNs,inputSeq;
  public Event(){} Event(long k,long seq){key=k;inputSeq=seq;sentNs=System.nanoTime();}}
 static void write(String dir,String name,String data)throws Exception{Files.writeString(Path.of(dir,name),data);}
 static String memory(){
  long gcCount=0,gcMs=0;for(GarbageCollectorMXBean b:ManagementFactory.getGarbageCollectorMXBeans()){gcCount+=Math.max(0,b.getCollectionCount());gcMs+=Math.max(0,b.getCollectionTime());}
  com.sun.management.OperatingSystemMXBean os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
  return "{\"at_ms\":"+System.currentTimeMillis()+",\"heap_used\":"+ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()+",\"nonheap_used\":"+ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed()+",\"cpu_ns\":"+os.getProcessCpuTime()+",\"gc_count\":"+gcCount+",\"gc_ms\":"+gcMs+"}";
 }
 static class Source extends RichParallelSourceFunction<Event>{
  final String input,dir;volatile boolean running=true;
  Source(String i,String d){input=i;dir=d;}
  public void run(SourceContext<Event> c)throws Exception{
   System.gc();write(dir,"baseline-memory.json",memory());
   while(running && !Files.exists(Path.of(dir,"run.start")))Thread.sleep(50);
   long start=System.nanoTime(),n=0;
   try(DataInputStream in=new DataInputStream(new BufferedInputStream(Files.newInputStream(Path.of(input)),1<<20))){
    while(running){long key;try{key=in.readLong();}catch(EOFException done){break;}
     synchronized(c.getCheckpointLock()){c.collect(new Event(key,n));}n++;
    }
   }
   write(dir,"source.json","{\"emitted\":"+n+",\"start_ns\":"+start+",\"end_ns\":"+System.nanoTime()+"}");
   boolean collected=false;
   while(running && !Files.exists(Path.of(dir,"finish"))){
    if(!collected && Files.exists(Path.of(dir,"gc.request"))){System.gc();write(dir,"retained-memory.json",memory());collected=true;}
    Thread.sleep(50);
   }
  }
  public void cancel(){running=false;}
 }
 static class State extends KeyedProcessFunction<Long,Event,Event>{
  final String dir;transient ValueState<Boolean> state;
  long unique,duplicates,count,firstNs,lastNs,latencySum,maxLatency;long[] buckets=new long[32];
  State(String d){dir=d;}
  public void open(OpenContext c)throws Exception{
   ValueStateDescriptor<Boolean> descriptor=new ValueStateDescriptor<>("seen",Types.BOOLEAN);
   descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(6)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build());
   state=getRuntimeContext().getState(descriptor);
   getRuntimeContext().getMetricGroup().gauge("benchProcessed",()->count);
   getRuntimeContext().getMetricGroup().gauge("benchUnique",()->unique);
   getRuntimeContext().getMetricGroup().gauge("benchDuplicates",()->duplicates);
  }
  public void processElement(Event e,Context c,Collector<Event> out)throws Exception{
   boolean isNew=state.value()==null;
   state.update(true); // refresh six-hour TTL on EVERY arrival (sliding inactivity horizon)
   if(isNew)unique++;else duplicates++;
   long now=System.nanoTime(),us=Math.max(0,(now-e.sentNs)/1000);if(count==0)firstNs=now;lastNs=now;count++;latencySum+=us;maxLatency=Math.max(us,maxLatency);
   buckets[us==0?0:Math.min(31,64-Long.numberOfLeadingZeros(us))]++;
   if(isNew)out.collect(e);
  }
  public void close()throws Exception{
   write(dir,"operator-"+getRuntimeContext().getIndexOfThisSubtask()+".json","{\"unique\":"+unique+",\"duplicates\":"+duplicates+",\"processed\":"+count+",\"first_ns\":"+firstNs+",\"last_ns\":"+lastNs+",\"latency_sum_us\":"+latencySum+",\"max_latency_us\":"+maxLatency+",\"latency_log2_buckets\":"+Arrays.toString(buckets)+"}");
  }
 }
 static class Sink extends RichSinkFunction<Event>{
  final String dir;transient DataOutputStream output;long count,latencySum,maxLatency;long[] buckets=new long[32];
  Sink(String d){dir=d;}
  public void open(OpenContext c)throws Exception{output=new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Path.of(dir,"output-"+getRuntimeContext().getIndexOfThisSubtask()+".bin")),1<<20));}
  public void invoke(Event e,Context c)throws Exception{
   long us=Math.max(0,(System.nanoTime()-e.sentNs)/1000);latencySum+=us;maxLatency=Math.max(maxLatency,us);buckets[us==0?0:Math.min(31,64-Long.numberOfLeadingZeros(us))]++;
   output.writeLong(e.key);output.writeLong(e.inputSeq);count++;
  }
  public void close()throws Exception{output.close();write(dir,"sink-"+getRuntimeContext().getIndexOfThisSubtask()+".json","{\"outputs\":"+count+",\"latency_sum_us\":"+latencySum+",\"max_latency_us\":"+maxLatency+",\"latency_log2_buckets\":"+Arrays.toString(buckets)+"}");}
 }
 public static void main(String[] a)throws Exception{
  String backend=a[0],dir=Path.of(a[1]).toAbsolutePath().toString(),input=a[2];int p=Integer.parseInt(a[3]),port=Integer.parseInt(a[4]);
  Files.createDirectories(Path.of(dir));
  Thread monitor=new Thread(()->{try(BufferedWriter w=Files.newBufferedWriter(Path.of(dir,"jvm.jsonl"))){while(true){w.write(memory());w.newLine();w.flush();Thread.sleep(1000);}}catch(Exception e){}});monitor.setDaemon(true);monitor.start();
  Configuration c=new Configuration();
  c.setString("rest.bind-address","127.0.0.1");c.setString("rest.address","127.0.0.1");c.setInteger("rest.port",port);c.setString("rest.bind-port",Integer.toString(port));
  c.setString("taskmanager.memory.managed.size","512mb");c.setString("taskmanager.memory.network.min","64mb");c.setString("taskmanager.memory.network.max","64mb");
  c.setString("state.backend.rocksdb.localdir",Path.of(dir,"rocksdb").toString());c.setInteger("state.checkpoints.num-retained",3);
  c.setString("restart-strategy.type","none");c.setString("metrics.fetcher.update-interval","1000");
  StreamExecutionEnvironment env=StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(c);env.setParallelism(p);env.setMaxParallelism(128);
  env.setStateBackend(backend.equals("heap")?new HashMapStateBackend():new EmbeddedRocksDBStateBackend(backend.equals("rocks-incremental")));
  env.enableCheckpointing(5000);env.getCheckpointConfig().setCheckpointTimeout(120000);env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
  env.getCheckpointConfig().setCheckpointStorage(Path.of(dir,"checkpoints").toUri().toString());
  env.addSource(new Source(input,dir)).name("synthetic-source").setParallelism(1).keyBy(e->e.key)
   .process(new State(dir)).name("six-hour-id-dedup").addSink(new Sink(dir)).name("downstream-file");
  env.execute("state-backend-"+backend);
 }
}
