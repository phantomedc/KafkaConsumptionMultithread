package net.smartleon.kafka.multithreading.consumermethodone;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerGroup {
    private List<ConsumerRunnable> consumers;
    public ConsumerGroup(int consumerNum,String groupId,String topic,String brokerList){
        consumers = new ArrayList<>(consumerNum);
        for(int i = 0;i < consumerNum;++i){
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList,groupId,topic);
            consumers.add(consumerThread);
        }
    }
    public void execute(){
        for(ConsumerRunnable task:consumers){
            new Thread(task).start();
        }
    }
}
