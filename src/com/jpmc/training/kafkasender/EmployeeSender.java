package com.jpmc.training.kafkasender;

import com.jpmc.training.domain.Employee;
import com.jpmc.training.serializer.EmployeeSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class EmployeeSender {
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props=new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerializer.class.getName());

        KafkaProducer<String, Employee> producer=new KafkaProducer<>(props);

        ProducerRecord<String, Employee> rec1=new ProducerRecord<String, Employee>("emp-topic", "emp-1",
                new Employee(5001, "Rajiv", "Developer"));

        ProducerRecord<String, Employee> rec2=new ProducerRecord<String, Employee>("emp-topic", "emp-2",
                new Employee(5002, "Suresh", "Accountant"));

        producer.send(rec1);
        producer.send(rec2);
        System.out.println("employee objects sent");
        producer.close();

    }
}
