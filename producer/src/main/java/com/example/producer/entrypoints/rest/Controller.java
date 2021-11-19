package com.example.producer.entrypoints.rest;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;
import com.example.producer.core.ports.SaveStudentsUseCase;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@AllArgsConstructor
public class Controller {

    private final SaveStudentsUseCase saveStudentsUseCase;

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


}
