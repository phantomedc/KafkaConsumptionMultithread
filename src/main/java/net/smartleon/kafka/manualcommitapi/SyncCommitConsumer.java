package net.smartleon.kafka.manualcommitapi;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by smartleon on 2018-03-11 0011.
 */
public class SyncCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","133.71.24.219:9092");
        props.put("group.id","manual_sync_hyz_test");
        // 是否自动提交offset,本类中设置为false
        props.put("enable.auto.commit","false");
        props.put("session.timeout.ms","30000");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("hyz_test"));
        try {
            while (true) {
                ConsumerRecords<String, String> cRecords = consumer.poll(100);
//                System.out.println("===========手动同步提交offset:"+SyncCommitConsumer.class.getName());
                for (ConsumerRecord<String, String> cRecord : cRecords) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s \n", cRecord.partition(), cRecord.offset(), cRecord.key(), cRecord.value());
                }
                // 手动提交offset
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    // 具体失败处理-你的应用应该处理这个错误, 回滚自从上次成功提交offset以来的消费的message造成的改变.
                }
            }
        }finally {
            consumer.close();
        }
    }
}
