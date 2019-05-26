package com.kafka.stream.processor;


import com.alibaba.fastjson.JSONArray;
import com.kafka.stream.util.PropertyReaderUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;

import java.util.*;
import java.util.stream.IntStream;

/**
 * 构建拓扑图
 */
public class MyTopologyWordCount {
    private static Map<String, Object> props = new HashMap<>();
    static PropertyReaderUtil reader = new PropertyReaderUtil();

    //加载配置文件
    static {

        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverMap.get("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application-recurt");//相当于kafka的group_id属性
        //kafka stream 0.10后新增的属性,允许通过实现org.apache.kafka.streams.cn.zcbigdata.recruitmentdata.processor.TimestampExtractor接口自定义记录时间
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);//并行线程数
    }

    /**
     * 动态构造拓扑图
     */
    public static void dynamic() {
        JSONArray jsonArray = reader.readYmlFile("topicConfig.yml");
        List<String> processors = new ArrayList<>();
        IntStream.range(0, jsonArray.size())
                .mapToObj(jsonArray::getJSONObject)
                .forEach(p -> {
                    String topic = p.getString("topic");
                    JSONArray processorArray = p.getJSONArray("processor");
                    if (processorArray != null) {
                        processorArray.stream().filter(Objects::nonNull).forEach(q -> processors.add(q.toString()));
                    }
                    topology(topic);
//                    dynamicTopology(topic,processors);
                });
    }

    /**
     * 构建固定的拓扑图
     */
    public static void topology(String topic) {
        TopologyBuilder builder = new TopologyBuilder();
        String parentName = "source";
        builder.addSource(parentName, topic);
//        builder.addProcessor("addName", AddNameProcessor::new,parentName);
        builder.addProcessor("CountsTest", wordCount::new, parentName);
//        builder.addStateStore(Stores.create("Counts").withStringKeys().withStringValues().inMemory().build(),"CountsTest");
        builder.addStateStore(Stores.create("Counts").withStringKeys().withStringValues().inMemory().build(), "CountsTest");
        builder.addSink("sink", "processor_out", "CountsTest");
        StreamsConfig streamsConfig = new StreamsConfig(props);
        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.start();
    }

    /**
     * 通过配置文件及工厂动态构建拓扑图
     *
     * @param processors
     */
    public static void dynamicTopology(String topic, List<String> processors) {
        PropertyReaderUtil propertyReaderUtil = new PropertyReaderUtil();
        Map<String, String> map = propertyReaderUtil.readPropertyFile("processor.properties");
        TopologyBuilder builder = new TopologyBuilder();
        String parentName = "source";
        builder.addSource("source", topic);
        for (String process : processors) {
            //通过构造器动态创建Processor实例
            String processCLass = map.get(process);
            if (!"".equals(processCLass)) {
                builder.addProcessor(process, new ProcessorSupplierFactory(processCLass), parentName);
            }
            parentName = process;
        }
        //将数据存入状态厂库
        //builder.addStateStore(Stores.create("word").withStringKeys().withIntegerValues().inMemory().build(),"counts");
        //builder.addSink("sink","processor_out_count",parentName);
        builder.addSink("sink1", "addName", parentName);
        StreamsConfig streamsConfig = new StreamsConfig(props);
        KafkaStreams kafkaStreams = new KafkaStreams(builder, streamsConfig);
        kafkaStreams.start();
    }
}
