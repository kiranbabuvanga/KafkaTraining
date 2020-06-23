package com.kafka.ww;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
   public static void main(String[] args) throws Exception {

	  String topicName = "SampleTest4";
	  
      Properties props = new Properties();      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test1");
      props.put("enable.auto.commit", "false");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("auto.offset.reset", "earliest");
      //System.out.println("111");
      
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);      
      consumer.subscribe(Arrays.asList(topicName));
      //System.out.println("222");
      
      int i = 0;      
      while (true) {
    	 //System.out.println("333");
         @SuppressWarnings("deprecation")
		 ConsumerRecords<String, String> records = consumer.poll(100);
         //System.out.println("444");
         for (ConsumerRecord<String, String> record : records)
         {
        	 //System.out.println("Record : "+record);
        	 System.out.printf("Topic = "+record.topic()+"\t"+"Partition = "+record.partition()+"\t"+"Offset = "+record.offset()+"\t"+"Key = "+record.key()+"\t"+"Value = "+record.value()+"\n");
         }       
      }
   }
}