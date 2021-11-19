package com.example.producer.entrypoints.rest;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class StudentOutput {

    private UUID uuid;
    private String consumerName;

}
