package com.jpmc.training.kafkasender;

import com.jpmc.training.partitioner.MessagePartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SenderWithCustomPartitioner {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MessagePartitioner.class.getName());
        KafkaProducer<String, String> producer=new KafkaProducer<>(props);

        for(int i=1001;i<=1010;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic-v", "key-1","This is a test mesage "+i);
            producer.send(record);
        }

        for(int i=2001;i<=2010;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic-v", "key-2","This is a test mesage "+i);
            producer.send(record);
        }
        for(int i=3001;i<=3010;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic-v", "key-3","This is a test mesage "+i);
            producer.send(record);
        }
        for(int i=4001;i<=4010;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic-v", "key-4","This is a test mesage "+i);
            producer.send(record);
        }
        for(int i=5001;i<=5010;i++) {
            ProducerRecord<String, String> record=new ProducerRecord<String, String>("test-topic-v", "key-5","This is a test mesage "+i);
            producer.send(record);
        }
        producer.close();

    }

}
