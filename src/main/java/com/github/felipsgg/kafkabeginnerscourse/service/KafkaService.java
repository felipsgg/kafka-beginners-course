package com.github.felipsgg.kafkabeginnerscourse.service;

public interface KafkaService {

    void producerSimpleDemo(String message);

    void producerDemoWithCallback(String message, Integer iterations);

}
