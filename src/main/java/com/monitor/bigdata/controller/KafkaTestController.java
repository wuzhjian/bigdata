package com.monitor.bigdata.controller;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * @author 44644
 */
@RestController
public class KafkaTestController {

    @Resource
    private KafkaProducer producer;

    @Resource
    private KafkaConsumer kafkaConsumer;

    public String topic = "test";
    // public String topic = "spark-kafka-demo-2";
    @RequestMapping(value = "/asynSend")
    public void asynSend(){
        long start = System.currentTimeMillis();

        for (int i = 0; i < 200000000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(i));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        // 发送成功
                        // System.out.println(metadata.topic().toString() +"   " + metadata.partition() + "    " + metadata.offset() + "    " + metadata.timestamp());
                    } else {
                        if (exception instanceof RetriableException){
                            // 可重试
                        } else {
                            // 不可重试
                        }
                    }
                }
            });
        }
        System.out.println("一共好事:  ===== "+(System.currentTimeMillis() - start));
    }

    @RequestMapping(value = "/synSend")
        public void synSend(){
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(i));
            try {
                Future<RecordMetadata> recordMetadataFuture = producer.send(record);

                // RecordMetadata recordMetadata = (RecordMetadata) producer.send(record).get(1000, TimeUnit.MICROSECONDS);
                // System.out.println(recordMetadata.topic().toString() +"   " + recordMetadata.partition() + "    " + recordMetadata.offset() + "    " + recordMetadata.timestamp());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("一共好事:  ===== "+(System.currentTimeMillis() - start));
    }

    @RequestMapping(value = "/consumer")
    public void consumerTest(){
        kafkaConsumer.subscribe(Arrays.asList(topic));
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.key() + "   " + record.value() + "   " + record.topic()
                + "   " +record.partition() + "   " + record.offset());
            }
        }
    }
}
