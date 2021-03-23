package com.jpmc.training.kafkasender;

import com.jpmc.training.domain.Employee;
import com.jpmc.training.partitioner.EmployeePartitioner;
import com.jpmc.training.serializer.EmployeeSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class EmployeeSenderWithCustomPartitioner {
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props=new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, EmployeePartitioner.class.getName());

        KafkaProducer<String, Employee> producer=new KafkaProducer<>(props);

        for(int i=1001;i<=1010;i++) {
            Employee e=new Employee(i, "Name "+i, "Developer");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        for(int i=2001;i<=2010;i++) {
            Employee e=new Employee(i, "Name "+i, "Accountant");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        for(int i=3001;i<=3010;i++) {
            Employee e=new Employee(i, "Name "+i, "Architect");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        for(int i=4001;i<=4010;i++) {
            Employee e=new Employee(i, "Name "+i, "Project Manager");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        for(int i=5001;i<=5010;i++) {
            Employee e=new Employee(i, "Name "+i, "Sys Admin");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        for(int i=6001;i<=6010;i++) {
            Employee e=new Employee(i, "Name "+i, "Business Analyst");
            ProducerRecord<String, Employee> record=new ProducerRecord<String, Employee>("emp-topic", e);
            producer.send(record);
        }
        System.out.println("employee objects sent");
        producer.close();

    }

}
