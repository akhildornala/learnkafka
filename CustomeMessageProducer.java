package com.deloitte.kafka.learnkafka.producer;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomeMessageProducer {

	public static void main(String args[]) {
		Properties props = new Properties();

		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("kafka.topic.name", "testtopic6");

		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props, new StringSerializer(),
				new ByteArraySerializer());
		Scanner sc = new Scanner(System.in);
		
		System.out.print("please enter input");
		String line = sc.nextLine();
		while (!line.equals("exit")) {
			System.out.println("please enter input");
			byte[] payload = line.getBytes();
			ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(
					props.getProperty("kafka.topic.name"), payload);
			producer.send(record);
			line = sc.nextLine();
		}
		System.out.print("Thank You");
		sc.close();
		producer.close();
	}

}