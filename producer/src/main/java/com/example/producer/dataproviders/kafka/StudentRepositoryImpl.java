package com.example.producer.dataproviders.kafka;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;
import com.example.producer.core.ports.StudentRepository;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@AllArgsConstructor
public class StudentRepositoryImpl implements StudentRepository {

    private final AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate;
    private final String requestTopic;

    @Override
    public List<StudentsConsumers> save(Student student) {

        StudentMessageInput studentInput = StudentMessageInput.newBuilder()
                .setName(student.getName())
                .setSurname(student.getSurname())
                .build();

        // create producer record
        ProducerRecord<String, StudentMessageInput> record = new ProducerRecord<>(requestTopic,studentInput);
        // post in kafka topic
        RequestReplyFuture<String, StudentMessageInput, Collection<ConsumerRecord<String, StudentMessageOutput>>> sendAndReceive = kafkaTemplate.sendAndReceive(record);
//        RequestReplyFuture<String, StudentMessageInput, StudentMessageOutput> sendAndReceive = kafkaTemplate.sendAndReceive(record);
        try {
            // confirm if producer produced successfully
            SendResult<String, StudentMessageInput> sendResult = sendAndReceive.getSendFuture().get();

            // get consumer record
            ConsumerRecord<String, Collection<ConsumerRecord<String, StudentMessageOutput>>> consumerRecord = sendAndReceive.get();
            System.out.println("Consumer record size "+consumerRecord.value().size());
            // return consumer value

            Iterator<ConsumerRecord<String, StudentMessageOutput>> consumerRecordIterator = consumerRecord.value().iterator();

            List<StudentsConsumers> studentsConsumers = new ArrayList<>();

            while (consumerRecordIterator.hasNext()){

                StudentMessageOutput studentMessageOutput = consumerRecordIterator.next().value();

                studentsConsumers.add(
                        StudentsConsumers.builder()
                                .uuid(UUID.fromString(studentMessageOutput.getUuid()))
                                .consumerName(studentMessageOutput.getConsumerName())
                                .build()
                );
                System.out.println(studentsConsumers);
            }

            return studentsConsumers;

        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalArgumentException(e);
        }

    }
}
