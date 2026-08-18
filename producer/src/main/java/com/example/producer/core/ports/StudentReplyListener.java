package com.example.producer.core.ports;

import com.example.producer.core.model.StudentsConsumers;

public interface StudentReplyListener {

    void onReply(StudentsConsumers studentsConsumers);

    void onComplete();

}
