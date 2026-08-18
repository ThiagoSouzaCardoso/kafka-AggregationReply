package com.example.producer.entrypoints.rest;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;
import com.example.producer.core.ports.SaveStudentsUseCase;
import com.example.producer.core.ports.StreamStudentsUseCase;
import com.example.producer.core.ports.StudentReplyListener;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@AllArgsConstructor
public class Controller {

    private static final long SSE_TIMEOUT_MILLIS = 15_000L;

    private final SaveStudentsUseCase saveStudentsUseCase;
    private final StreamStudentsUseCase streamStudentsUseCase;

    @PostMapping("/students")
    @ResponseStatus(HttpStatus.CREATED)
    public List<StudentOutput> save(@RequestBody StudentInput request) {
        Student student = Student.builder().name(request.getName()).surname(request.getSurname()).build();
        List<StudentsConsumers> studentsConsumers = saveStudentsUseCase.execute(student);
        List<StudentOutput> studentOutputs = studentsConsumers.stream()
                .map(studentsConsumers1 -> StudentOutput.builder()
                        .uuid(studentsConsumers1.getUuid())
                        .consumerName(studentsConsumers1.getConsumerName())
                        .build())
                .collect(Collectors.toList());


        return studentOutputs;
    }

    @GetMapping(value = "/students/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam String name, @RequestParam String surname) {
        Student student = Student.builder().name(name).surname(surname).build();
        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT_MILLIS);

        streamStudentsUseCase.execute(student, new StudentReplyListener() {
            @Override
            public void onReply(StudentsConsumers studentsConsumers) {
                try {
                    emitter.send(SseEmitter.event()
                            .name("student-reply")
                            .data(StudentOutput.builder()
                                    .uuid(studentsConsumers.getUuid())
                                    .consumerName(studentsConsumers.getConsumerName())
                                    .build()));
                } catch (IOException e) {
                    emitter.completeWithError(e);
                }
            }

            @Override
            public void onComplete() {
                emitter.complete();
            }
        });

        return emitter;
    }

}
