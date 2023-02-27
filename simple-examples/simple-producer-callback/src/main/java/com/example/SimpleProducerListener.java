package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;


@Slf4j
@SpringBootApplication
public class SimpleProducerListener {
    @Value("${app.kafka.topic}")
    private String topic;
    @Value("${app.kafka.broker-list}")
    private String brokerList;

    public static void main(String[] args) {
        SpringApplication.run(SimpleProducerListener.class, args);
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public ProducerListener<String, String> listener() {
        return new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata metadata) {
                log.info("Received new metadata. \n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset() + "\n" +
                        "Timestamp: " + metadata.timestamp());
            }

            @Override
            public void onError(ProducerRecord<String, String> producerRecord, RecordMetadata metadata,
                         Exception exception) {
                exception.printStackTrace();
                log.warn("Exception {}",exception);
            }
        };
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
		KafkaTemplate<String, String>  template =  new KafkaTemplate<>(producerFactory());
		template.setProducerListener(listener());
		return template;
    }

    @Bean
    @Profile("default") // Don't run from test(s)
    public ApplicationRunner runner() {
        var template = kafkaTemplate();

        return args -> IntStream.range(1, 30).forEach((i) -> {
            template.send(topic, "Line " + i);
        });
    }


}

