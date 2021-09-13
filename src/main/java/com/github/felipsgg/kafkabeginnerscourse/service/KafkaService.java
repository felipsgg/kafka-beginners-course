package com.github.felipsgg.kafkabeginnerscourse.service;

import java.util.concurrent.ExecutionException;

public interface KafkaService {

    void producerSimpleDemo(String message);

    void producerDemoWithCallback(String message, Integer iterations);

    void producerDemoWithKeys(String message) throws ExecutionException, InterruptedException;

    void consumerSimpleDemo();

    void consumerDemoAssignSeek();

    void realWorldExample();

}
