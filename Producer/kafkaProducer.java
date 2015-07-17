package com.amadeus.kafka.training;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/* A Simple producer using API. This producer is creating log messages and sending 
to a topic called vulab123 on a node with name vulab-build-system
Please make sure vulab-build-system is configured in the /etc/hosts file in your unix or linux environment
*/

 
public class KafkaProducer {
    public static void main(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();
 
        Properties props = new Properties();
        // set the broker address
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("producer.type", "sync");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.amadeus.kafka.training.KafkaPartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               System.out.println("creating event "+nEvents);
               long runtime = new Date().getTime();  
               String ip = "192.168.2."+ rnd.nextInt(255); 
               String msg = runtime + ",www.amadeus.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
               producer.send(data);
        }
        producer.close();
    }
}