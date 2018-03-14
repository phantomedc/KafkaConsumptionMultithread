package net.smartleon.kafka.multithreading.consumermanual;

/**
 * Created by smartleon on 2018-03-13 0013.
 */
public class Main {
    public static void main(String[] args) {
        String brokerList ="192.168.5.210:9092";
        String topic = "hyz_test_10p";
        String groupID = "test_group";
        final ConsumerThreadHandler<byte[],byte[]> handler = new ConsumerThreadHandler<>(brokerList,groupID,topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                handler.consume(cpuCount);
            }
        };
        new Thread(runnable).start();
        try{
            // 20秒后自动停止测试程序
            Thread.sleep(200000L);
        }catch (InterruptedException e){
            // swallow this exception
        }
        System.out.println("Starting to close the consumer ...");
        handler.close();

    }
}
