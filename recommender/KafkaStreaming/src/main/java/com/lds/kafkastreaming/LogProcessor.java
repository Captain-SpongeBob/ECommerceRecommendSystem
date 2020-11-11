package com.lds.kafkastreaming;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context;

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        // 根据前缀过滤日志信息，提取后面的内容
        if (input.contains("PRODUCT_RATING_PREFIX:")) {
            System.out.println("product rating coming!!!!" + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    public void punctuate(long timestamp) {
    }

    public void close() {
    }
}