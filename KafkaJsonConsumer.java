package com.deloitte.kafka.learnkafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.deloitte.kafka.learnkafka.producer.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonConsumer {

	static String TOPIC = "wellsjsontopic";
	static String GROUP = "group1";

	public static void main(String args[]) {
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
		kafkaConsumer.subscribe(Arrays.asList(TOPIC));
		final ObjectMapper objectMapper = new ObjectMapper();

		try {
			while (true) {
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = kafkaConsumer.poll(5000L);
				System.out.println("size:" + records.count());
				if (records.count() > 0) {
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("record is:" + record.value());
						Employee emp = objectMapper.readValue(record.value(), Employee.class);
						System.out.println(emp);
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
