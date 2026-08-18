package com.example.producer.core.usecases;

import com.example.producer.core.model.Student;
import com.example.producer.core.ports.StreamStudentsUseCase;
import com.example.producer.core.ports.StudentReplyListener;
import com.example.producer.core.ports.StudentRepository;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StreamStudentsUseCaseImpl implements StreamStudentsUseCase {

    private final StudentRepository studentRepository;

    @Override
    public void execute(Student student, StudentReplyListener listener) {
        studentRepository.save(student, listener);
    }
}
