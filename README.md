# KafkaConsumptionMultithread
**特别注意由于kafka的配置没有配置：**
advertised.host.name=<hostname routable by clients>
这里应该配置主机名，否则kafka会以主机名访问
第二种解决办法：在运行kafka消费者的主机的hosts文件中配置IP地址与主机名的映射
192.168.5.210 kafka64

kafka启动
bin/kafka-server-start.sh -daemon config/server.properties &

kafka-topics.sh --list --zookeeper 127.0.0.1:2181

kafka-topics.sh --delete --zookeeper 127.0.0.1:2181 --topic test

--创建topic
kafka-topics.sh --create --zookeeper  127.0.0.1:2181 --replication-factor 1 --partitions 4 --topic test

kafka-topics.sh --describe --zookeeper  127.0.0.1:2181 --topic test

kafka-console-producer.sh --broker-list  127.0.0.1:9092 --topic test

kafka-console-consumer.sh --zookeeper  127.0.0.1:2181 --from-beginning --topic test

写入一些string数据的最简单的方法就是通过kafka-verifiable-producer.sh脚本. 你需要确保topic有超过一个partition. 
kafka-verifiable-producer.sh --topic test --max-messages 200 --broker-list 192.168.5.210:9092


--查看topic消费进度
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 127.0.0.1:9092 --topic test --time -1
    -1表示查询test各个分区当前最大的消息位移值(注意，这里的位移不只是consumer端的位移，而是指消息在每个分区的位置)


#彻底删除Kafka中的topic - CSDN博客  http://blog.csdn.net/fengzheku/article/details/50585972
1、删除kafka存储目录（server.properties文件log.dirs配置，默认为"/tmp/kafka-logs"）相关topic目录

2、Kafka 删除topic的命令是：

     ./bin/kafka-topics  --delete --zookeeper 【zookeeper server】  --topic 【topic name】

     如果kafaka启动时加载的配置文件中server.properties没有配置delete.topic.enable=true，那么此时的删除并不是真正的删除，而是把topic标记为：marked for deletion
     你可以通过命令：./bin/kafka-topics --zookeeper 【zookeeper server】 --list 来查看所有topic

     此时你若想真正删除它，可以如下操作：

     （1）登录zookeeper客户端：命令：./bin/zookeeper-client

     （2）找到topic所在的目录：ls /brokers/topics

     （3）找到要删除的topic，执行命令：rmr /brokers/topics/【topic name】即可，此时topic被彻底删除。

    另外被标记为marked for deletion的topic你可以在zookeeper客户端中通过命令获得：ls /admin/delete_topics/【topic name】，
    如果你删除了此处的topic，那么marked for deletion 标记消失
    zookeeper 的config中也有有关topic的信息： ls /config/topics/【topic name】暂时不知道有什么用

总结：

彻底删除topic：

 1、删除kafka存储目录（server.properties文件log.dirs配置，默认为"/tmp/kafka-logs"）相关topic目录

 2、如果配置了delete.topic.enable=true直接通过命令删除，如果命令删除不掉，直接通过zookeeper-client 删除掉broker下的topic即可。
# The default number of log partitions per topic. More partitions allow greater
# parallelism for consumption, but this will also result in more files across
# the brokers.
每个Topic的默认日志分区数。更多的分区允许更大的并行性用于消费，但是这也会导致更多的文件通过Broker。
num.partitions=1
保证分区数大于1，测试多线程消费


java -jar zb2hnkafka.jar test 192.168.5.210:9092 earliest

--监控consumer group的消费进度

kafka-consumer-groups.sh --bootstrap-server 192.168.5.210:9092 --describe --group test-group
需要添加--new-consumer 
kafka-consumer-groups.sh  --new-consumer --bootstrap-server 192.168.5.210:9092 --describe --group test-group