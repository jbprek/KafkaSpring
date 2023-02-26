package com.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Console;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

@SpringBootApplication
public class SimpleProducer {
	@Value("${app.kafka.topic}")
	private String topic;
	@Value("${app.kafka.broker-list}")
	private String brokerList;

	public static void main(String[] args) {
		SpringApplication.run(SimpleProducer.class, args);
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
	public ProducerFactory<Integer, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}



	@Bean
	public KafkaTemplate<Integer, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	@Profile("default") // Don't run from test(s)
	public ApplicationRunner runner() {
		var template = kafkaTemplate();

		return args -> {
			System.out.println("Hit ^C to terminate...");
			while(true) {

				Scanner scanner = new Scanner(System.in);

				System.out.print("> ");
				String line = scanner.nextLine();
				template.send(topic, line);
				template.flush();
			}
		};
	}


}

