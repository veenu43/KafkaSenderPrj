package com.jpmc.training.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyCallback implements Callback {
    private String key;



    public MyCallback(String key) {
        super();
        this.key = key;
    }



    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        // TODO Auto-generated method stub

        if(exception==null) {
            System.out.println("message with id "+key+" delivered to partition "+metadata.partition()+" at offset "+metadata.offset());
        }

    }

}
