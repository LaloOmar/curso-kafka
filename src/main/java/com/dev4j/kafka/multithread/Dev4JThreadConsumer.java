package com.dev4j.kafka.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dev4JThreadConsumer extends Thread {
	private final KafkaConsumer<String, String> consumer;

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private static final Logger log = LoggerFactory.getLogger(Dev4JThreadConsumer.class);

	public Dev4JThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void run() {
		consumer.subscribe(Arrays.asList("dev4jtest-topic"));
		while (!closed.get()) {
			try {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("patition = {},offset = {} ,key = {} , value = {}", consumerRecord.partition(),
							consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
				}

			} catch (WakeupException e) {
				if (!closed.get()) {
					throw e;
				}
			} finally {
				consumer.close();
			}
		}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

}
