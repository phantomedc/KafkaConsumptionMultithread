package net.smartleon.kafka.multithreading.consumermanual;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerWorker<K,V> implements Runnable {
    private final ConsumerRecords<K,V> records;
    private final Map<TopicPartition,OffsetAndMetadata> offsets;

    public ConsumerWorker(ConsumerRecords<K,V> record,Map<TopicPartition,OffsetAndMetadata> offsets){
        this.records = record;
        this.offsets = offsets;
    }

    @Override
    public void run() {
        for(TopicPartition partition:records.partitions()){
            List<ConsumerRecord<K,V>> partitionRecords = records.records(partition);
            for(ConsumerRecord<K,V> record : partitionRecords){
                // 插入消息处理逻辑，本例只是打印消息
                System.out.println(String.format("topic=%s,partition=%d,offset=%d",record.topic(),record.partition(),record.offset()));
            }
            // 上报位移信息
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            synchronized(offsets){
                if(!offsets.containsKey(partition)){
                    offsets.put(partition,new OffsetAndMetadata(lastOffset + 1));
                }else{
                    long curr = offsets.get(partition).offset();
                    if(curr <=lastOffset + 1){
                        offsets.put(partition,new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
    }
}
