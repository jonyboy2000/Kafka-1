package com.amadeus.kafka.training;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

/* A Simple producer using API. This producer is creating log messages and sending 
 to a topic called vulab123 on a node with name vulab-build-system
 Please make sure vulab-build-system is configured in the /etc/hosts file in your unix or linux environment
 */

public class KafkaProducer {
	public static void main(String[] args) throws IOException {
		long events = Long.parseLong(args[0]);
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", "127.0.0.1:9092");
		props.put("producer.type", "sync");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.vulab.kafka.training.VulabKafkaPartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		Configuration configuration = new Configuration();
		configuration.addResource(
				"/opt/cloudera/parcels/CDH-5.4.2-1.cdh5.4.2.p0.2/lib/hadoop-0.20-mapreduce/conf/core-site.xml");
		configuration.addResource(
				"/opt/cloudera/parcels/CDH-5.4.2-1.cdh5.4.2.p0.2/lib/hadoop-0.20-mapreduce/conf/hdfs-site.xml");
		configuration.set("fs.defaultFS", "hdfs://ovhbhshad1:8020/");

		FileSystem dfs = FileSystem.get(configuration);

		Path pt = new Path("/user/yyan/dac_sample");
		BufferedReader br = new BufferedReader(new InputStreamReader(dfs.open(pt)));
		String line;
		line = br.readLine();
		while (line != null) {
			System.out.println(line);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("amadeus123", line);
			producer.send(data);
			line = br.readLine();
		}
		producer.close();
	}
}
