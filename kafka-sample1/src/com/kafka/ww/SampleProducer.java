package com.kafka.ww;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducer {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS");  
		Date date;
		
		String topicName = "SampleTest4";
		List colorResponseList = new ArrayList();
		List courseResponseList = new ArrayList();
		
	    List colorList = new ArrayList();
	    colorList.add("black");
	    colorList.add("white");
	    colorList.add("red");
	    colorList.add("blue");
	    colorList.add("yellow");
	    
	    List courseList = new ArrayList();
	    courseList.add("java");
	    courseList.add("mulesoft");
	    courseList.add("kafka");
		
	    Properties props = new Properties();
	    props.put("bootstrap.servers", "localhost:9092");
	    props.put("acks", "all");
	    props.put("retries", 0);
	    props.put("batch.size", 16384);
	    props.put("linger.ms", 1);
	    props.put("buffer.memory", 33554432);
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    
	    Producer<String, String> producer = new KafkaProducer<String, String>(props);
	    
	    for(int i = 30; i < 40; i++)
	    {
	    	//RecordMetadata colorResponse = producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get();
	    	//colorResponseList.add(colorResponse);
	    	date = new Date();
	    	System.out.println(producer.send(new ProducerRecord<String, String>(topicName, "ColorCount"+i)).get());
	    	System.out.println("String Message sent successfully"+formatter.format(date));
	    }
	    
	    /*for(int i = 21; i < 30; i++)
	    {
	    	//RecordMetadata colorResponse = producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get();
	    	//colorResponseList.add(colorResponse);
	    	date = new Date();
	    	System.out.println(producer.send(new ProducerRecord<String, String>(topicName, 0, "Color", "ColorCount"+i)).get());
	    	System.out.println("String Message sent successfully"+formatter.format(date));
	    }
	    
	    for(int j = 121; j < 130; j++)
	    {
	    	//RecordMetadata colorResponse = producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get();
	    	//colorResponseList.add(colorResponse);
	    	date = new Date();
	    	ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, 1, "Place", "PlaceCount"+j);
	    	producer.send(record,
	    			new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception e) {
							// TODO Auto-generated method stub
							if(e != null) {
			                    e.printStackTrace();
			                 } else {
			                    System.out.println("The offset of the record we just sent is: " + metadata);
			                 }							
						}
				    });
	    	System.out.println("String Message sent successfully"+formatter.format(date));
	    }
	    
	    for(int i = 1021; i < 1030; i++)
	    {
	    	//RecordMetadata colorResponse = producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get();
	    	//colorResponseList.add(colorResponse);
	    	date = new Date();
	    	System.out.println(producer.send(new ProducerRecord<String, String>(topicName, 2, "Subject", "SubjectCount"+i)).get());
	    	System.out.println("String Message sent successfully"+formatter.format(date));
	    }*/
	    	          
	    /*for(int i = 0; i < colorList.size(); i++)
	    {
	    	//RecordMetadata colorResponse = producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get();
	    	//colorResponseList.add(colorResponse);
	    	System.out.println(producer.send(new ProducerRecord<String, String>(topicName, 3, "Color", (String) colorList.get(i))).get());
	    }
	    System.out.println("colorResponseList : "+colorResponseList);
	    
	    for(int j = 0; j < courseList.size(); j++)
	    {
	    	//RecordMetadata courseResponse = producer.send(new ProducerRecord<String, String>(topicName, 2, "Color", (String) courseList.get(j))).get();
	    	//courseResponseList.add(courseResponse);
	    	System.out.println(producer.send(new ProducerRecord<String, String>(topicName, 2, "Color", (String) courseList.get(j))).get());
	    }
	    System.out.println("courseResponseList : "+courseResponseList);*/
	   
	    
	    /*System.out.println(producer.send(new ProducerRecord<String, String>(topicName, "Color", "Violet")).get());	    
	    System.out.println("String Message sent successfully"+formatter.format(date));
	    
	    System.out.println(producer.send(new ProducerRecord<String, String>(topicName, "Color", "Indigo")).get());	    
	    System.out.println("String Message sent successfully"+formatter.format(date));*/
	    
	    
	    /*ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, "Color", "Green");
	    producer.send(record,
	    			new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception e) {
							// TODO Auto-generated method stub
							if(e != null) {
			                    e.printStackTrace();
			                 } else {
			                    System.out.println("The offset of the record we just sent is: " + metadata);
			                 }							
						}
				    }
	    		);*/
	    				 
	    
	    producer.close();

	}

}

