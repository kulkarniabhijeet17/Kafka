package com.javase.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;

public class Main {
	public static void main(String[] args) {
		// Configure Consumer
		Properties consumerProps = getKafkaConsumerProperties();

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList("input-topic"));

		// Configure Producer
		Properties producerProps = getKafkaProducerProperties();

		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					// Assume the record value is JSON with a "type" field
					String recordValue = record.value();
					if (recordValue.contains("\"type\":\"A\"")) {
						System.out.println("Processing Type A: " + recordValue);
						// Send to another topic or process further
					} else if (recordValue.contains("\"type\":\"B\"")) {
						System.out.println("Processing Type B: " + recordValue);
						// Send to another topic or process further
					}
				}
			}
		} finally {
			consumer.close();
			producer.close();
		}
	}

	private static Properties getKafkaConsumerProperties() {
		Properties props = loadKafkaProperties();

		Properties consumerProps = new Properties();
		consumerProps.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
		consumerProps.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty("group.id"));
		consumerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().getClass());
		consumerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().getClass());
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Manual offset management

		return consumerProps;
	}

	private static Properties getKafkaProducerProperties() {
		Properties props = loadKafkaProperties();

		Properties producerProps = new Properties();
		producerProps.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("bootstrap.servers"));
		producerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().getClass());
		producerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().getClass());

		return producerProps;
	}

	private static Properties loadKafkaProperties() {
		Properties props = new Properties();
		try {
			InputStream input = Main.class.getClassLoader().getResourceAsStream("kafka.properties");
			props.load(input);
		} catch (IOException ex) {
			System.out.println("Exception occurred: " + ex.getMessage());
		}
		return props;
	}
}