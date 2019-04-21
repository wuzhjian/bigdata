package com.monitor.bigdata.conf;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author 44644
 */
@Configuration
public class KafkaConfig {

    @Value("${kafka.producer.servers}")
    private String servers;

    @Value("${kafka.producer.acks}")
    private String acks;

    @Value("${kafka.producer.retries}")
    private String retries;

    @Value("${kafka.producer.batch.size}")
    private String batchSize;

    /*@Value("${kafka.producer.linger}")
    private int lingers;*/

    @Value("${kafka.producer.buffer.memory}")
    private String buffer;

    @Value("${kafka.producer.max.block.ms}")
    private String blockMs;


    public Properties producerProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put("retries", retries);
        props.put("batch.size", batchSize);
        props.put("linger.ms", 100);
        props.put("buffer.memory", buffer);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, blockMs);

        return props;
    }

    @Bean
    public KafkaProducer<String, String> producer(){
        return new KafkaProducer<String, String>(producerProperties());
    }

    @Value("${kafka.consumer.servers}")
    private String consumerServers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;

    @Value("${kafka.consumer.auto.commit.interval}")
    private String autoCommitInterval;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    public Properties consumerProperties(){

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public KafkaConsumer<String, String > consumer(){
        return new KafkaConsumer<String, String>(consumerProperties());
    }
}
