package com.example.producer.configurations;

import com.example.producer.core.ports.StudentRepository;
import com.example.producer.dataproviders.kafka.StudentRepositoryImpl;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class RepositoryConfig {

    @Bean
    public StudentRepository studentRepository(AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate ,
                                               @Value("${kafka.topic.request-topic}") String requestTopic){
        return new StudentRepositoryImpl(kafkaTemplate,requestTopic);
    }


}
