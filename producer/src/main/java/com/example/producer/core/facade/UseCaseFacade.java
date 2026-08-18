package com.example.producer.core.facade;

import com.example.producer.core.ports.SaveStudentsUseCase;
import com.example.producer.core.ports.StreamStudentsUseCase;
import com.example.producer.core.ports.StudentRepository;
import com.example.producer.core.usecases.SaveStudentsUseCaseImpl;
import com.example.producer.core.usecases.StreamStudentsUseCaseImpl;

public class UseCaseFacade {

    public static SaveStudentsUseCase saveStudentsUseCase(StudentRepository studentRepository){
        return new SaveStudentsUseCaseImpl(studentRepository);
    }

    public static StreamStudentsUseCase streamStudentsUseCase(StudentRepository studentRepository){
        return new StreamStudentsUseCaseImpl(studentRepository);
    }


}
