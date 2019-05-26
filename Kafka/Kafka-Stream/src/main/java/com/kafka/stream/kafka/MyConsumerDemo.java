package com.kafka.stream.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kafka.stream.util.PropertyReaderUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static com.kafka.stream.common.MongoUtils.mongodbInsert;


/**
 * @author haojiewu
 * @version V1.0.0
 * @time 2018/11/27
 * @description :kafka消费者
 */
public class MyConsumerDemo {
    static PropertyReaderUtil reader = new PropertyReaderUtil(); //读取配置文件

    public static void main(String[] args) {
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        Properties props = new Properties();
        props.put("bootstrap.servers", serverMap.get("bootstrap.servers"));
        props.put("group.id", serverMap.get("group.id"));
        props.put("enable.auto.commit", serverMap.get("enable.auto.commit"));
        props.put("auto.commit.interval.ms", serverMap.get("auto.commit.interval.ms"));
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", serverMap.get("auto.offset.reset"));
        props.put("key.deserializer", serverMap.get("key.deserializer"));
        props.put("value.deserializer", serverMap.get("value.deserializer"));

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
        //读取哪个topic
//        consumer.subscribe(Arrays.asList("processor_out"));
        consumer.subscribe(Arrays.asList("demo"));
        while (true) {
//            autoOffset(consumer);
            toMongo(consumer);
        }
    }

    /**
     * 使用消费者消费数据
     *
     * @param consumer
     */
    public static void autoOffset(KafkaConsumer<String, Long> consumer) {
        ConsumerRecords<String, Long> records = consumer.poll(100);
        records.forEach(p -> System.out.printf("offset = %d, key = %s, value = %s%n", p.offset(), p.key(), p.value()));
    }

    /**
     * 数据输出至Mongodb
     *
     * @param consumer
     */
    public static void toMongo(KafkaConsumer<String, Long> consumer) {
        ConsumerRecords<String, Long> records = consumer.poll(100);
        records.forEach(r -> {
            JSONObject jsonObject = JSON.parseObject(String.valueOf(r.value()));
            mongodbInsert(new Document(jsonObject));
        });
    }

    /**
     * 在Application调用consumer消费
     */
    public static void toTest() {
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        Properties props = new Properties();
        props.put("bootstrap.servers", serverMap.get("bootstrap.servers"));
        props.put("group.id", serverMap.get("group.id"));
        props.put("enable.auto.commit", serverMap.get("enable.auto.commit"));
        props.put("auto.commit.interval.ms", serverMap.get("auto.commit.interval.ms"));
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", serverMap.get("auto.offset.reset"));
        props.put("key.deserializer", serverMap.get("key.deserializer"));
        props.put("value.deserializer", serverMap.get("value.deserializer"));

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
        //读取哪个topic
//        consumer.subscribe(Arrays.asList("processor_out"));
        consumer.subscribe(Arrays.asList("demo"));
        while (true) {
//            autoOffset(consumer);
            toMongo(consumer);
        }
    }
}
