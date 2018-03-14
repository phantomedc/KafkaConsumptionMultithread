package net.smartleon.kafka.multithreading.consumermethodone;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerRunnable implements Runnable {
    // 每个线程维护私有的KafkaConsumer实例
    private final KafkaConsumer<String,String> consumer;

    public ConsumerRunnable(String brokerList,String groupId,String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit","true");// 本例使用自动提交offset
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic)); // 本例使用分区副本自动分配策略
    }
    @Override
    public void run() {
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(200); // 本例使用200ms作为获取超时时间
            for(ConsumerRecord<String,String> record:records){
                // 这里写消息处理逻辑，本例中只是简单地打印消息
                System.out.println(Thread.currentThread().getName()+" consumed "+record.partition()+"the message with offset:"+record.offset());
            }
        }
    }
}
