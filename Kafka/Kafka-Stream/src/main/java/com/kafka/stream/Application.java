package com.kafka.stream;



import com.kafka.stream.kafka.MyConsumerDemo;
import com.kafka.stream.processor.MyTopology;
import com.kafka.stream.processor.MyTopologyWordCount;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {

    public static void main(String[] args) {
//        ExecutorService consumer = Executors.newSingleThreadExecutor();
//        consumer.submit(()-> MyConsumerDemo.toTest());
        //数据清洗
        ExecutorService stream1 = Executors.newSingleThreadExecutor();
        stream1.submit(()-> MyTopology.dynamic());
//        //单词统计
//        ExecutorService stream2 = Executors.newSingleThreadExecutor();
//        stream2.submit(()-> MyTopologyWordCount.dynamic());
    }
}
