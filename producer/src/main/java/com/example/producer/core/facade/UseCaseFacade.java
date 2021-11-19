package com.example.producer.core.facade;

import com.example.producer.core.ports.SaveStudentsUseCase;
import com.example.producer.core.ports.StudentRepository;
import com.example.producer.core.usecases.SaveStudentsUseCaseImpl;

public class UseCaseFacade {

    public static SaveStudentsUseCase saveStudentsUseCase(StudentRepository studentRepository){
        return new SaveStudentsUseCaseImpl(studentRepository);
    }


}
