package com.example.producer.core.ports;

import com.example.producer.core.model.Student;
import com.example.producer.core.model.StudentsConsumers;

import java.util.List;

public interface StudentRepository {

    List<StudentsConsumers> save(Student student);

}
