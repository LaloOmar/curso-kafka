package com.dev4j.kafka.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {
	private static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");// para transaccional debe ser all
		props.put("transactional.id", "dev4jtest-Producer-Id");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "0");
		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			try {
				producer.initTransactions();
				producer.beginTransaction();

				for (int i = 0; i < 10000; i++) {
					producer.send(new ProducerRecord<>("dev4jtest-topic", "dev4jkey-" + i, "valor*" + i));
					if(i==5000) {
						throw new Exception("Unexpected Exception");
					}
				}
				producer.commitTransaction();
				producer.flush();
			} catch (Exception e) {
				// TODO: handle exception
				log.error("Error", e);
				producer.abortTransaction();
			}

		}
	}
}
