package net.smartleon.kafka.multithreading.consumermanual;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerThreadHandler<K,V> {
    private final KafkaConsumer<K,V> consumer;
    private ExecutorService executors;
    private final Map<TopicPartition,OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList,String groupId,String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit","false");// 手动提交offset此值设置为false
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsets.clear();
            }
        });
    }
    /**
     * 消费主方法
     * @param threadNumber 线程池中线程数
     */
    public void consume(int threadNumber){
        executors = new ThreadPoolExecutor(
                threadNumber,
                threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try{
            while(true){
                ConsumerRecords<K,V> records = consumer.poll(1000L);
                if(!records.isEmpty()){
                    executors.submit(new ConsumerWorker<>(records,offsets));
                }
                commitOffsets();
            }
        }catch(WakeupException e){
            // swallow this exception
        }
    }
    private void commitOffsets(){
        // 尽量降低synchronized块对offsets锁定时间
        Map<TopicPartition,OffsetAndMetadata> unmodfiedMap;
        synchronized(offsets){
            if(offsets.isEmpty()){
                return;
            }
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodfiedMap);
    }
    public void close(){
        consumer.wakeup();
        executors.shutdown();
    }
}
