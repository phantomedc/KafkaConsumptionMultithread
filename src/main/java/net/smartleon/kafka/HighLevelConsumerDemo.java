package net.smartleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by smartleon on 2018-03-08 0008.
 */
public class HighLevelConsumerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.5.210:9092");
        props.put("group.id","hyz_test");
        // 是否自动提交
        props.put("enable.auto.commit","true");
        // 自动提交间隔
        props.put("auto.commit.interval.ms","1000");
        //coordinator维护的timer.当timer过期时, 如果还没有心跳收到, coordinator会将这个成员标记为挂掉, 然后通知其他组内其他成员需要重新分配分区
        props.put("session.timeout.ms","30000");
        props.put("auto.offset.reset","earliest");//设置offset位置-earliest从最早开始
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("hyz_test"));
        while(true){
            ConsumerRecords<String,String> cRecords = consumer.poll(100);
            for (ConsumerRecord<String,String> cRecord:cRecords){
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s \n", cRecord.partition(), cRecord.offset(), cRecord.key(), cRecord.value());
            }
        }
    }
}
