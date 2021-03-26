package com.jpmc.training.kafkasender;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SecureSender {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props=new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9010");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty("security.protocol","SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism","PLAIN");


        KafkaProducer<String, String> producer=new KafkaProducer<>(props);

        for(int i=1;i<=10;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-security-topic", "key-1","This is a test mesage "+i);
            producer.send(record);
        }
        producer.close();

    }

}
