package com.kafka.stream.processor;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.stream.Stream;

/**
 * @author haojiewu
 * @version V1.0.0
 * @time 2018/11/28
 * @description : 单词技术统计
 * Processor<String, String> 入参是两个string类型，那么在MyTopologyWordCount类中需要设置映射
 *  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 *  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 */
public class wordCount implements Processor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        //context.getStateStore，对应在MyTopologyWordCount拓扑图中应添加 builder.addStateStore(Stores.create("Counts")
        this.kvStore = (KeyValueStore<String, String>) context.getStateStore("Counts");
    }



    @Override
    public void process(String key, String value) {
        System.out.println("===>:"+value);
        Stream.of(value.toLowerCase().split(" ")).forEach(word->{

            int count = kvStore.get(word) == null ? 1 : Integer.valueOf(kvStore.get(word)) + 1;
            kvStore.put(word,String.valueOf(count));
        });
    }
    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, String> iterator = this.kvStore.all();
        iterator.forEachRemaining(entry -> {
            System.out.println(entry.key+":"+entry.value);
            context.forward(entry.key, entry.value);
            this.kvStore.delete(entry.key);
        });
        context.commit();
    }
    @Override
    public void close() {
        this.kvStore.close();
    }

}