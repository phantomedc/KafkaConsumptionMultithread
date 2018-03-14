package net.smartleon.kafka.manualcommitapi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by smartleon on 2018-03-12 0012.
 */
public class SyncCommitCOffsetPartitionFor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","133.71.24.219:9092");
        props.put("group.id","manual_sync_hyz_test");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        String topic ="hyz_test";
        KafkaConsumer<String,String> consumer =new KafkaConsumer<>(props);
        try{
            // 手动分配分区-将topic下的所有分区分配给consumer
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo partition:consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(topic,partition.partition()));
            }
            consumer.assign(partitions);
            while(true){
                ConsumerRecords<String,String> cRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> cRecord : cRecords) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s \n", cRecord.partition(), cRecord.offset(), cRecord.key(), cRecord.value());
                    // 同步提交offset会等待数据处理完后再提交
                    consumer.commitSync(Collections.singletonMap(new TopicPartition(cRecord.topic(),cRecord.partition()),new OffsetAndMetadata(cRecord.offset()+1)));
                }
            }
        }finally {
            consumer.close();
        }
    }
}
