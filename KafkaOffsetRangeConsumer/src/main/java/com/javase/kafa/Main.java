package com.javase.kafa;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class Main {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "my-offset-consumer-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false"); // Manual offset management

		String topicName = "my-topic";
		int partition = 0; // Assuming you want to consume from partition 0
		long startOffset = 6; // Starting offset
		long endOffset = 10; // Ending offset (exclusive)

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		try {
			// Assign the specific partition
			TopicPartition topicPartition = new TopicPartition(topicName, partition);
			consumer.assign(Collections.singletonList(topicPartition));

			// Seek to the desired starting offset
			consumer.seek(topicPartition, startOffset);

			// Poll records until the end offset is reached
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				if (records.isEmpty()) {
					continue;
				}

				boolean reachedEndOffset = false;
				for (ConsumerRecord<String, String> record : records) {
					if (record.offset() >= endOffset) {
						reachedEndOffset = true;
						break; // Stop processing once end offset is reached
					}
					System.out.printf("Consumed record: Partition = %d, Offset = %d, Key = %s, Value = %s%n",
							record.partition(), record.offset(), record.key(), record.value());
					// Process the record here
				}

				// Commit offsets manually after processing
				consumer.commitSync();

				if (reachedEndOffset) {
					break; // Exit the loop if end offset is reached
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}