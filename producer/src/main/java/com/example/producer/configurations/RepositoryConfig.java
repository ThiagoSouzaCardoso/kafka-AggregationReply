package com.example.producer.configurations;

import com.example.producer.core.ports.StudentRepository;
import com.example.producer.dataproviders.kafka.StudentRepositoryImpl;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class RepositoryConfig {

    @Bean
    public StudentRepository studentRepository(AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate,
                                               @Value("${kafka.topic.request-topic}") String requestTopic,
                                               @Value("${kafka.topic.requestreply-topic}") String replyTopic,
                                               @Value("${kafka.consumers.expected-count}") int expectedRepliesCount,
                                               @Value("${kafka.consumers.stream-reply-timeout-seconds}") long streamReplyTimeoutSeconds,
                                               ScheduledExecutorService sseTimeoutScheduler){
        return new StudentRepositoryImpl(kafkaTemplate, requestTopic, replyTopic,
                expectedRepliesCount, streamReplyTimeoutSeconds, sseTimeoutScheduler);
    }

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService sseTimeoutScheduler() {
        return Executors.newScheduledThreadPool(2);
    }

}
