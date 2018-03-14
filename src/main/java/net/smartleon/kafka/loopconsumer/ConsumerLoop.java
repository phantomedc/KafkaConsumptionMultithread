package net.smartleon.kafka.loopconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by smartleon on 2018-03-08 0008.
 */
public class ConsumerLoop implements Runnable {
    private final KafkaConsumer<String,String> consumer;
    private final List<String>  topics;
    private final int id;

    public ConsumerLoop(int id,String groupId,List<String> topics){
        this.id = id;
        this.topics = topics;
        Properties props=new Properties();
        props.put("bootstrap.servers","192.168.5.210:9092");
        props.put("group.id",groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<String, String>(props);
    }
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.out.println(this.id + ": " + data);
                }
            }
        }catch(WakeupException e){
            // ignore for shutdown
        }finally {
            consumer.close();
        }
    }
    public void shutdown(){
        consumer.wakeup();
    }
}
