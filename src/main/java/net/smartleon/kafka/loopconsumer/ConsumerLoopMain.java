package net.smartleon.kafka.loopconsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by smartleon on 2018-03-08 0008.
 */
public class ConsumerLoopMain {
    public static void main(String[] args) {
        int numConsumers = 3;
        String groupId = "consuer-tutorial-group2";
        List<String> topics = Arrays.asList("test");
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0;i<numConsumers;i++){
            ConsumerLoop consumer = new ConsumerLoop(i,groupId,topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                for (ConsumerLoop consumer:consumers){
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
