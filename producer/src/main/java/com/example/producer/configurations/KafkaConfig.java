package com.example.producer.configurations;

import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.requestreply-topic}")
    private String requestReplyTopic;


    @Bean
    public AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput>
    aggregatingReplyingKafkaTemplate(ProducerFactory<String, StudentMessageInput> pf,
                                     KafkaMessageListenerContainer<String, StudentMessageOutput> container,
                                     BiPredicate<Collection<ConsumerRecord<String, StudentMessageOutput>>, Boolean> releaseStrategy){

        AggregatingReplyingKafkaTemplate kafkaTemplate = new AggregatingReplyingKafkaTemplate(pf, container, releaseStrategy);
        kafkaTemplate.setDefaultReplyTimeout(Duration.ofSeconds(10));
        kafkaTemplate.setReturnPartialOnTimeout(true);
        return kafkaTemplate;
    }

    @Bean
    public BiPredicate<Collection<ConsumerRecord<String, StudentMessageOutput>>, Boolean> releaseStrategy(
            @Value("${seila}") int releaseSize){
        AtomicInteger releaseCount = new AtomicInteger();
      return  (list, timeout) -> {
            releaseCount.incrementAndGet();
            return timeout ? true :  list.size() == releaseSize;
        };
//        return (consumerRecords, aBoolean) -> true;
    }


    @Bean
    public KafkaMessageListenerContainer<String, StudentMessageOutput> replyContainer(ConsumerFactory<String, StudentMessageInput> cf) {
        ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return new KafkaMessageListenerContainer(cf, containerProperties);
    }


//    @Bean
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, StudentMessageOutput>> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, StudentMessageOutput> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        return factory;
//    }

//    @Bean
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, StudentMessageOutput>> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, StudentMessageOutput> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        return factory;
//    }



//    public AggregatingReplyingKafkaTemplate<Integer, String, String> aggregatingTemplate(
//            TopicPartitionOffset topic, int releaseSize, AtomicInteger releaseCount) {
//        //Create Container Properties
//        ContainerProperties containerProperties = new ContainerProperties(topic);
//        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//        //Set the consumer Config
//        //Create Consumer Factory with Consumer Config
//        DefaultKafkaConsumerFactory<Integer, Collection<ConsumerRecord<Integer, String>>> cf =
//                new DefaultKafkaConsumerFactory<>(consumerConfigs());
//
//        //Create Listener Container with Consumer Factory and Container Property
//        KafkaMessageListenerContainer<Integer, Collection<ConsumerRecord<Integer, String>>> container =
//                new KafkaMessageListenerContainer<>(cf, containerProperties);
//        //  container.setBeanName(this.testName);
//        AggregatingReplyingKafkaTemplate<Integer, String, String> template =
//                new AggregatingReplyingKafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerConfigs()), container,
//                        (list, timeout) -> {
//                            releaseCount.incrementAndGet();
//                            return list.size() == releaseSize;
//                        });
//        template.setSharedReplyTopic(true);
//        template.start();
//        return template;
//    }







//    @Bean
//    public ReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> replyKafkaTemplate(ProducerFactory<String, StudentMessageInput> pf, KafkaMessageListenerContainer<String, StudentMessageOutput> container){
//        return new ReplyingKafkaTemplate(pf, container);
//    }
//
//    @Bean
//    public KafkaMessageListenerContainer<String, StudentMessageOutput> replyContainer(ConsumerFactory<String, StudentMessageInput> cf) {
//        ContainerProperties containerProperties = new ContainerProperties(requestReplyTopic);
//        return new KafkaMessageListenerContainer(cf, containerProperties);
//    }
//    @Bean
//    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, StudentMessageOutput>> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, StudentMessageOutput> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        return factory;
//    }


}
