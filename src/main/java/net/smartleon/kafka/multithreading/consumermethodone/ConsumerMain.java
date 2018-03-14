package net.smartleon.kafka.multithreading.consumermethodone;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "192.168.5.210:9092";
        String groupId="testGroup1";
        String topic = "hyz_test";
        int consumerNum = 3;
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum,groupId,topic,brokerList);
        consumerGroup.execute();
    }
}
