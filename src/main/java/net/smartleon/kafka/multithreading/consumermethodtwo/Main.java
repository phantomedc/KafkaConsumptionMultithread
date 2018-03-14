package net.smartleon.kafka.multithreading.consumermethodtwo;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class Main {
    public static void main(String[] args) {
        String brokerList ="192.168.5.210:9092";
        String groupId = "group2";
        String topic = "hyz_test";
        int workerNum = 5;

        ConsumerHandler consumer = new ConsumerHandler(brokerList,groupId,topic);
        consumer.execute(workerNum);
        try{
            Thread.sleep(1000000);
        }catch(InterruptedException ignored){
            consumer.shutdown();
        }
    }
}
