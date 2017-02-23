package com.zl.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jacky on 2017/2/21.
 */
public class MyKafkaConsumer {

    private static final String TOPIC = "new_topic";

    public static void main(String[] args) {

        // 设置配置属性
        Properties props = getKafkaConfig();

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        Integer number = 5;
        topicCountMap.put(TOPIC, number); // 一次从主题中获取数据条数

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streamList = messageStreams.get(TOPIC); // 获取每次接收到的数据

        System.out.println(streamList.size());

        // 启动所有线程
        ExecutorService executor = Executors.newFixedThreadPool(number);

        // 必须要启动多线程的方式消费消息，否则消费不到其它partiontion 的消息
        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> cit = stream.iterator();
                    while (cit.hasNext()) {
                        try {
                            MessageAndMetadata<byte[], byte[]> mam = cit.next();
                            String message = new String(mam.message());
                            System.out.println("Partition: " + mam.partition() + ",Message: " + message);
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

    }


    public static Properties getKafkaConfig() {

        Properties pro = new Properties();

        pro.put("group.id", "test-kafka-group");
        pro.put("zookeeper.connect", "192.168.43.161:2181,192.168.43.162:2181,192.168.43.163:2181");
        pro.put("zookeeper.session.timeout.ms", "4000");
        pro.put("zookeeper.sync.time.ms", "200");
        pro.put("auto.commit.interval.ms", "1000");
        pro.put("auto.offset.reset", "smallest");
        return pro;

    }
}
