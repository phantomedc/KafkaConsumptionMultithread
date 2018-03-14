package net.smartleon.kafka.manualcommitapi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 异步提交offset，consumer.commitAsync()带callback方法，每次读取100条(100ms延迟)，传入2000条记录，消费者输出确实没有明显的停顿
 * Created by smartleon on 2018-03-12 0012.
 */
public class AsyncCommitCOffset {
    public static void main(String[] args) {
       Properties props = new Properties();
       props.put("bootstrap.servers","133.71.24.219:9092");
       props.put("group.id","manual_async_hyz_test");
       // 是否自动提交=false
       props.put("enable.auto.commit","true");
       props.put("session.timeout.ms","30000");
       props.put("key.deserializer", StringDeserializer.class.getName());
       props.put("value.deserializer",StringDeserializer.class.getName());

       KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
       consumer.subscribe(Arrays.asList("hyz_test"));
       try{
           while(true){
               ConsumerRecords<String,String> cRecords = consumer.poll(100);
               for (ConsumerRecord<String,String> cRecord:cRecords){
                   System.out.printf("partition = %d,offset = %d, key = %s,value = %s \n",cRecord.partition(),cRecord.offset(),cRecord.key(),cRecord.value());
               }
               consumer.commitAsync(new OffsetCommitCallback() {
                   @Override
                   public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                       if (e!=null){
                           // 应用具体处理异常部分
                       }
                   }
               });
           }
       }finally {
           consumer.close();
       }
    }
}
