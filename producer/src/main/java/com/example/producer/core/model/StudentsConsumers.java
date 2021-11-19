package com.example.producer.core.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.UUID;

@Builder
@Getter
@ToString
public class StudentsConsumers {

    private UUID uuid;
    private String consumerName;

}
