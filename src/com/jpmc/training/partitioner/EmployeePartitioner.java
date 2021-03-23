package com.jpmc.training.partitioner;

import com.jpmc.training.domain.Employee;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class EmployeePartitioner  implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // TODO Auto-generated method stub
        int partition=4;
        Employee employee=(Employee)value;
        String designation=employee.getDesignation();
        if(designation.equals("Developer")) {
            partition=0;

        }
        else if(designation.equals("Accountant")) {
            partition=1;
        }
        else if(designation.equals("Architect")) {
            partition=2;
        }
        else if(designation.equals("Project Manager")) {
            partition=3;
        }


        return partition;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}