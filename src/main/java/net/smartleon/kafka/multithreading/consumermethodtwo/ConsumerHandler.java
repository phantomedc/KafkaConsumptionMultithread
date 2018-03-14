package net.smartleon.kafka.multithreading.consumermethodtwo;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerHandler {
    // 本例中使用一个consumer讲消息放入后端队列，你当然可以使用前一种方法中的多实例按照某张规则同时把消息放入后端队列
    private final KafkaConsumer<String,String> consumer;
    private ExecutorService executors;
    public ConsumerHandler(String brokerList,String groupId,String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("group.id",groupId);
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");
        props.put("session.timeout.ms","30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer",StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }
    public void execute(int workerNum){
        executors = new ThreadPoolExecutor(workerNum,workerNum,0L, TimeUnit.MILLISECONDS,new ArrayBlockingQueue<Runnable>(1000),new ThreadPoolExecutor.CallerRunsPolicy());
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(200);
            for(final ConsumerRecord record:records){
                executors.submit(new Worker(record));
            }
        }
    }
    public void shutdown(){
        if(consumer !=null){
            consumer.close();
        }
        if(executors!=null){
            executors.shutdown();
        }
        try{
            if(!executors.awaitTermination(10,TimeUnit.SECONDS)){
                System.out.println("Timeout ... Ignore for this case");
            }
        }catch(InterruptedException ignored){
            System.out.println("Other thread interruputed this shutdown, ignure for this case.");
            Thread.currentThread().interrupt();
        }
    }
}