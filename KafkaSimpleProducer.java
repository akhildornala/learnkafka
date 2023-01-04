package com.deloitte.kafka.learnkafka.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSimpleProducer {

	public static String TOPIC = "testtopic6";

	public static void main(String args[]) throws Exception {
		System.out.println("Welcome to producer");

		Properties props = new Properties();

		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("kafka.topic.name", TOPIC);

		props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		List<String> messages = getMessages();

		for (String message : messages) {
			producer.send(new ProducerRecord<>(TOPIC, null, message));
			System.out.printf("message %s sent\n", message);
			Thread.sleep(5000); // Demo only,
		}
		System.out.print("going to stop producer");
		producer.close();

	}

	private static List<String> getMessages() {
		List<String> messages = new ArrayList<>();
		messages.add("hey hi");
		messages.add("AKHIL DORNALA");
		messages.add("hello world");
		messages.add("welcome to Deloitte");
		messages.add("Hyderabad");
		messages.add("charminar");
		messages.add("happy new year 2023");
		messages.add("this is a sample kafka producer");
		messages.add("bye bye");
		return messages;
	}

}
