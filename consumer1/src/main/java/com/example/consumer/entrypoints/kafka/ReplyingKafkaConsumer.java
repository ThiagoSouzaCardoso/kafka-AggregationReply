package com.example.consumer.entrypoints.kafka;


import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.UUID;


@Component
public class ReplyingKafkaConsumer {
	 
	 @KafkaListener(topics = "${kafka.topic.request-topic}",id = "1",groupId = "D_REQUEST2")
	 @SendTo
	  public StudentMessageOutput listen(ConsumerRecord<String, StudentMessageInput> payload) throws InterruptedException, JsonProcessingException {
		 System.out.println(payload.value());
		 System.out.println("Consumer2");


		 StudentMessageOutput studentOutput = StudentMessageOutput.newBuilder()
				 .setConsumerName("Consumer2")
				 .setUuid(UUID.randomUUID().toString()).build();

		 return studentOutput;
	  }



}
