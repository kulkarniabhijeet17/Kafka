package com.javase.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {
	public static void main(String[] args) {
		// Configure Consumer
		Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", "localhost:9092");
		consumerProps.put("group.id", "channel-splitter-group");
		consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList("input-topic"));

		// Configure Producer
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", "localhost:9092");
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			records.forEach(record -> {
				String value = record.value();
				if (value.contains("\"channel\":\"Both\"")) {
					// Split into sms and email records
					String smsRecord = value.replace("\"channel\":\"Both\"", "\"channel\":\"sms\"");
					String emailRecord = value.replace("\"channel\":\"Both\"", "\"channel\":\"email\"");

					// Send to output topic(s)
					producer.send(new ProducerRecord<>("output-topic", record.key(), smsRecord));
					producer.send(new ProducerRecord<>("output-topic", record.key(), emailRecord));
				} else {
					// Forward as-is
					producer.send(new ProducerRecord<>("output-topic", record.key(), value));
				}
			});
		}
	}
}