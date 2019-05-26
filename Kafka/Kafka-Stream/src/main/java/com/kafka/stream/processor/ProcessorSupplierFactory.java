package com.kafka.stream.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author haojiewu
 * @desc processorSupplier工厂类
 * @create 18-4-18
 */
public class ProcessorSupplierFactory implements ProcessorSupplier {
    public static final Logger logger = LoggerFactory.getLogger(ProcessorSupplierFactory.class);

    private String processorName;
    public ProcessorSupplierFactory(String processorName){
        this.processorName = processorName;
    }
    @Override
    public Processor get() {
        Processor processor = null;
        Objects.requireNonNull(processorName);
        try {
            processor = (Processor) Class.forName(processorName).newInstance();
        } catch (InstantiationException e) {
            logger.error("反射类失败",e);
        } catch (IllegalAccessException e) {
            logger.error("错误：",e);
        } catch (ClassNotFoundException e) {
            logger.error("找不到类",e);
        }
        return processor;
    }
}
