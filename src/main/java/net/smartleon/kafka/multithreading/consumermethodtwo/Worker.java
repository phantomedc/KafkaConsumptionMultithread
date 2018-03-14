package net.smartleon.kafka.multithreading.consumermethodtwo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class Worker implements Runnable {
    private ConsumerRecord<String,String> consumerRecord;
    public Worker(ConsumerRecord record){
        this.consumerRecord=record;
    }
    @Override
    public void run() {
        // 这里写消息处理逻辑，本例中只是简单打印消息
        System.out.println(Thread.currentThread().getName()+" consumed "+consumerRecord.partition() + " the message with offset: "+consumerRecord.offset());
    }
}
