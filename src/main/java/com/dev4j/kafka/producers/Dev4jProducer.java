package com.dev4j.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Dev4jProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "0");
		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			for (int i = 0; i < 10; i++) {
				producer.send(new ProducerRecord<>("dev4jtest-topic", "dev4jkey-"+i, "valor*"+i));
			}
			producer.flush();
		}
	}

}
