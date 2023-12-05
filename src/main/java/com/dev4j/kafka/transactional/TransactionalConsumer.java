package com.dev4j.kafka.transactional;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalConsumer {
	private static final Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "dev4jtest-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("isolation.level", "read_committed");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Arrays.asList("dev4jtest-topic"));
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("patition = {},offset = {} ,key = {} , value = {}", consumerRecord.partition(),
							consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
				}
			}

		}

	}

}
