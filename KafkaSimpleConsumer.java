package com.deloitte.kafka.learnkafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaSimpleConsumer {
	
	static String TOPIC = "testtopic6";
	static String GROUP = "testtopic6_group1";

	public static void main(String args[]) {
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
		configProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
//		configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		kafkaConsumer.subscribe(Arrays.asList(TOPIC));

		try {
			while (true) {
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = kafkaConsumer.poll(1000L);
				System.out.println("size:" + records.count());
				if (records.count() > 0) {
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("record is:" + record.value());
					}
				}
				Thread.sleep(3000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
	}

}
