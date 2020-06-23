package com.kafka.consumergroup;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerGroup1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KafkaConsumer<String, String> consumer = null;
		
		String topicName = "SampleTest5";
		  
		Properties props = new Properties();      
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "CG1");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		//System.out.println("111");
	      
		consumer = new KafkaConsumer<String, String>(props);      
		consumer.subscribe(Arrays.asList(topicName));
		//System.out.println("222");
	      
		int i = 0;      
		try 
		{
			while (true) 
			{
				//System.out.println("333");
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = consumer.poll(100);
				//System.out.println("444");
				for (ConsumerRecord<String, String> record : records)
				{
					//System.out.println("Record : "+record);
					//System.out.printf("offset = %d, key = %s, value = %s\n",  record.offset(), record.key(), record.value());
					System.out.printf("Topic = "+record.topic()+"\t"+"Partition = "+record.partition()+"\t"+"Offset = "+record.offset()+"\t"+"Key = "+record.key()+"\t"+"Value = "+record.value()+"\n");
				}       
			}	
		} catch(Exception e) {
			System.out.println("Exception : "+e);
		} finally {
		    try {
		        //consumer.commitSync();
		        consumer.close();
		    } finally {
		        consumer.close();
		        System.out.println("Closed consumer and we are done");
		    }
		}
	}
}
