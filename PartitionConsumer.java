package com.deloitte.kafka.learnkafka.consumer;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class PartitionConsumer {

	public static String BOOTSTRAP_SERVERS = "localhost:9092";
	public static String TOPIC = "testtopic9";

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(GROUP_ID_CONFIG, "group1");

		props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(singletonList(TOPIC));

		System.out.println("Consumer Starting!");

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));

			if (records.count() == 0)
				continue;

			for (ConsumerRecord<String, String> record : records) {

				System.out.printf("partition= %d, offset= %d, key= %s, value= %s\n", record.partition(),
						record.offset(), record.key(), record.value());
			}
		}
	}

}
