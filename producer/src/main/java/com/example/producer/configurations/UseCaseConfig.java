package com.example.producer.configurations;

import com.example.producer.core.facade.UseCaseFacade;
import com.example.producer.core.ports.SaveStudentsUseCase;
import com.example.producer.core.ports.StudentRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {

    @Bean
    public SaveStudentsUseCase saveStudentsUseCase(StudentRepository studentRepository){
        return UseCaseFacade.saveStudentsUseCase(studentRepository);
    }





}
