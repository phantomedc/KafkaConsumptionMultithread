package net.smartleon.kafka.manualcommitapi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 处理完已经读取的某一个分区的消息后再commit offset
 * Created by smartleon on 2018-03-12 0012.
 */
public class SyncCommitCOffsetDonePartition {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","133.71.24.219:9092");
        props.put("group.id","manual_sync_hyz_test");
        // 是否自动提交-false
        props.put("enable.auto.commit","false");
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("hyz_test"));

        try{
            while(true){
                ConsumerRecords<String,String> cRecords = consumer.poll(100);
                for (TopicPartition partition:cRecords.partitions()){
                    List<ConsumerRecord<String,String>> partitionRecords = cRecords.records(partition);
                    for (ConsumerRecord cRecord:partitionRecords){
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s \n", cRecord.partition(), cRecord.offset(), cRecord.key(), cRecord.value());
                    }
                    long lastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,new OffsetAndMetadata(lastoffset + 1)));
                }
            }
        }finally {
            consumer.close();
        }
    }
}
