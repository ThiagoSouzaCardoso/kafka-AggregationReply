package com.example.producer.dataproviders.kafka;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;
import com.example.producer.core.ports.StudentRepository;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@AllArgsConstructor
public class StudentRepositoryImpl implements StudentRepository {

    private final AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate;
    private final String requestTopic;

    @Override
    public List<StudentsConsumers> save(Student student) {

        try {
        StudentMessageInput studentInput = StudentMessageInput.newBuilder()
                .setName(student.getName())
                .setSurname(student.getSurname())
                .build();

        return  kafkaTemplate.sendAndReceive(new ProducerRecord<>(requestTopic,studentInput)).get().value().stream()
                    .map(ConsumerRecord::value)
                    .map(cr -> StudentsConsumers.builder()
                            .consumerName(cr.getConsumerName())
                            .uuid(UUID.fromString(cr.getUuid()))
                            .build())
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
           throw new IllegalArgumentException(e);
        }
    }
}
