package com.deloitte.kafka.learnkafka.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PartitionProducer {

	public static String BOOTSTRAP_SERVERS = "localhost:9092";

	public static String TOPIC = "testtopic9";
	
	public static void main(String[] args) throws Exception {
		System.out.println("Welcome to producer");

		Properties props = new Properties();

		
		props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

		props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// Key as string, value as string
		Producer<String, String> producer = new KafkaProducer<>(props);
		List<String> messages = getMessages();
		for (int i = 0; i <= 10; i++) {
			for (String message : messages) {
				producer.send(new ProducerRecord<>(TOPIC, "key1", message));
				System.out.printf("Greeting %s sent\n", message);
				Thread.sleep(5000);
			}
		}

		
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







