package com.example.producer.core.ports;

import com.example.producer.core.model.Student;

public interface StreamStudentsUseCase {

    void execute(Student student, StudentReplyListener listener);

}
