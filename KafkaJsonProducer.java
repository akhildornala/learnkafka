package com.deloitte.kafka.learnkafka.producer;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.SerializationUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJsonProducer {

	public static String TOPIC = "wellsjsontopic";
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

	public static void main(String args[]) throws Exception {
		System.out.println("Welcome to producer");

		Properties props = new Properties();

		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("kafka.topic.name", TOPIC);

		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props, new StringSerializer(),
				new ByteArraySerializer());

		
		Employee emp = new Employee(1, "akhildornala", "CLOUDENG");

		final ObjectMapper objectMapper = new ObjectMapper();
		byte[] payload = objectMapper.writeValueAsBytes(emp);

		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(
				props.getProperty("kafka.topic.name"), payload);
		producer.send(record);
		System.out.print("going to stop producer");
		producer.close();

	}

}

