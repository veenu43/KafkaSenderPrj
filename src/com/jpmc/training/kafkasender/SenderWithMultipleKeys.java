package com.jpmc.training.kafkasender;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SenderWithMultipleKeys {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Properties props=new Properties();
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer=new KafkaProducer<>(props);
		
		for(int i=1001;i<=1010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-1","This is a test mesage "+i);
			producer.send(record);
		}
		
		for(int i=2001;i<=2010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-2","This is a test mesage "+i);
			producer.send(record);
		}
		for(int i=3001;i<=3010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-3","This is a test mesage "+i);
			producer.send(record);
		}
		for(int i=4001;i<=4010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-4","This is a test mesage "+i);
			producer.send(record);
		}
		for(int i=5001;i<=5010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-5","This is a test mesage "+i);
			producer.send(record);
		}
		
		for(int i=6001;i<=6010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-6","This is a test mesage "+i);
			producer.send(record);
		}
		for(int i=7001;i<=7010;i++) {
			ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic", "key-7","This is a test mesage "+i);
			producer.send(record);
		}
		producer.close();

	}

}
