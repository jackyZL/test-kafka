package com.zl.producer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by jacky on 2017/2/21.
 */
public class MyKafkaProducer {

    private static final String TOPIC = "new_topic";

    public static void main(String[] args) throws InterruptedException {

        Producer producer = new Producer<String, String>(new ProducerConfig(getKafkaConfig()));


        for (int i = 0; i < 100; i++) {

            //Thread.sleep(1000);

            // 使用同样的key发送数据, 默认会发往同一个partition
            //producer.send(new KeyedMessage<String, String>(TOPIC, "key", "the same key : value-->" + i));


            Random rnd = new Random();

            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;

            System.out.println("send message :" + msg);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, ip, msg);

            producer.send(data);


            //System.out.println("send message:" + "value" + i);
        }

        producer.close();
    }

    public static Properties getKafkaConfig() {

        Properties props = new Properties();

        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "192.168.43.161:9092,192.168.43.162:9092,192.168.43.163:9092");

        // 设置zk
        props.put("zookeeper.connect", "192.168.43.161:2181,192.168.43.162:2181,192.168.43.163:2181");

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        props.put("request.required.acks", "-1");

        props.put("partitioner.class", "com.zl.partion.SimplePartitioner");

        return props;

    }


}
