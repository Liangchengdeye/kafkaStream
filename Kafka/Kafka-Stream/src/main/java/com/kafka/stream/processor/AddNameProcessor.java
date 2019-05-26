package com.kafka.stream.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * 数据清洗
 */
public class AddNameProcessor implements Processor {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        JSONObject jsonObject = JSON.parseObject(o2.toString());
        System.out.println("原：=======>"+jsonObject);
        JSONObject jsNew = dataClean(jsonObject);
        System.out.println("清洗：==>"+jsNew.toString());
        context.forward(o.toString(),jsNew.toString());
        context.commit();
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }

    /**
     * 清洗
     * @param inputStream
     * @return
     */
    public static JSONObject dataClean(JSONObject inputStream){
        String companyUrl = inputStream.getString("companyUrl");
        String companyName = inputStream.getString("companyName");
        String source = inputStream.getString("source");
        String workAddress = inputStream.getString("workAddress");
        String positionName = inputStream.getString("positionName");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("companyName",companyName);
        jsonObject.put("positionName",positionName);
        jsonObject.put("companyUrl",companyUrl);
        jsonObject.put("source",source);
        jsonObject.put("workAddress",workAddress);
        return jsonObject;
    }
}
