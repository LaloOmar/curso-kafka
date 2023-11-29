package com.dev4j.kafka.multithread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Dev4jMultithreadConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "dev4jtest-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		ExecutorService executor = Executors.newFixedThreadPool(5);

		for (int i = 0; i < 5; i++) {
			Dev4JThreadConsumer consumer = new Dev4JThreadConsumer(new KafkaConsumer<>(props));
			executor.execute(consumer);
		}
		while (executor.isTerminated())
			;
	}

}
