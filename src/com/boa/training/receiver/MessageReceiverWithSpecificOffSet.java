package com.boa.training.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class MessageReceiverWithSpecificOffSet {
	public static void main(String[] args) {
		Properties props=new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("group.id", "test-group");
		KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(props);
	/*	List<String> topicList=new ArrayList<String>();
		topicList.add("test-topic");
		consumer.subscribe(topicList);*/
		List<TopicPartition> partitionList=new ArrayList<TopicPartition>();
		//int partitionNumber=Integer.parseInt(args[0]);
		TopicPartition partition=new TopicPartition("msg-topic",0);
		partitionList.add(partition);
		consumer.assign(partitionList);
		consumer.seek(partition,0);
		System.out.println("assigned to msg-topic");
		while(true){
			ConsumerRecords<String, String> records=consumer.poll(Duration.ofSeconds(3));
			for(ConsumerRecord<String, String> record:records){
				System.out.println("message received with key: "+record.key()+" and value: "+record.value()+"From Parttiorion "+record.partition()+ "at offset"+record.offset() );
			}
		}
	}
}
