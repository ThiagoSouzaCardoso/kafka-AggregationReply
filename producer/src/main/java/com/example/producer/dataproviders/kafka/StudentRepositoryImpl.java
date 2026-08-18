package com.example.producer.dataproviders.kafka;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;
import com.example.producer.core.ports.StudentReplyListener;
import com.example.producer.core.ports.StudentRepository;
import io.confluent.developer.StudentMessageInput;
import io.confluent.developer.StudentMessageOutput;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.requestreply.AggregatingReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class StudentRepositoryImpl implements StudentRepository {

    private final AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate;
    private final String requestTopic;
    private final String replyTopic;
    private final int expectedRepliesCount;
    private final long streamReplyTimeoutSeconds;
    private final ScheduledExecutorService scheduler;

    private final Map<String, PendingStream> pendingStreams = new ConcurrentHashMap<>();

    public StudentRepositoryImpl(AggregatingReplyingKafkaTemplate<String, StudentMessageInput, StudentMessageOutput> kafkaTemplate,
                                  String requestTopic,
                                  String replyTopic,
                                  int expectedRepliesCount,
                                  long streamReplyTimeoutSeconds,
                                  ScheduledExecutorService scheduler) {
        this.kafkaTemplate = kafkaTemplate;
        this.requestTopic = requestTopic;
        this.replyTopic = replyTopic;
        this.expectedRepliesCount = expectedRepliesCount;
        this.streamReplyTimeoutSeconds = streamReplyTimeoutSeconds;
        this.scheduler = scheduler;
    }

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

    @Override
    public void save(Student student, StudentReplyListener listener) {
        String correlationId = UUID.randomUUID().toString();

        StudentMessageInput studentInput = StudentMessageInput.newBuilder()
                .setName(student.getName())
                .setSurname(student.getSurname())
                .build();

        ProducerRecord<String, StudentMessageInput> record = new ProducerRecord<>(requestTopic, studentInput);
        record.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, replyTopic.getBytes(StandardCharsets.UTF_8)));

        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> complete(correlationId),
                streamReplyTimeoutSeconds, TimeUnit.SECONDS);
        pendingStreams.put(correlationId, new PendingStream(listener, timeoutTask));

        kafkaTemplate.send(record);
    }

    // Unique per instance, same reasoning as replyContainer() in KafkaConfig: each pod
    // must see every reply on this topic since the pending correlation ids it's tracking
    // only exist in its own memory.
    @KafkaListener(topics = "${kafka.topic.requestreply-topic}", groupId = "sse-dispatcher-${kafka.consumer.instance-id}")
    public void onSseReply(ConsumerRecord<String, StudentMessageOutput> record) {
        Header correlationHeader = record.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
        if (correlationHeader == null) {
            return;
        }

        String correlationId = new String(correlationHeader.value(), StandardCharsets.UTF_8);
        PendingStream pendingStream = pendingStreams.get(correlationId);
        if (pendingStream == null) {
            return;
        }

        StudentMessageOutput output = record.value();
        pendingStream.getListener().onReply(StudentsConsumers.builder()
                .consumerName(output.getConsumerName())
                .uuid(UUID.fromString(output.getUuid()))
                .build());

        if (pendingStream.getRepliesReceived().incrementAndGet() >= expectedRepliesCount) {
            complete(correlationId);
        }
    }

    private void complete(String correlationId) {
        PendingStream pendingStream = pendingStreams.remove(correlationId);
        if (pendingStream == null) {
            return;
        }
        pendingStream.getTimeoutTask().cancel(false);
        pendingStream.getListener().onComplete();
    }

    private static class PendingStream {

        private final StudentReplyListener listener;
        private final ScheduledFuture<?> timeoutTask;
        private final AtomicInteger repliesReceived = new AtomicInteger();

        private PendingStream(StudentReplyListener listener, ScheduledFuture<?> timeoutTask) {
            this.listener = listener;
            this.timeoutTask = timeoutTask;
        }

        private StudentReplyListener getListener() {
            return listener;
        }

        private ScheduledFuture<?> getTimeoutTask() {
            return timeoutTask;
        }

        private AtomicInteger getRepliesReceived() {
            return repliesReceived;
        }
    }
}
