package com.github.felipsgg.kafkabeginnerscourse.web;

import com.github.felipsgg.kafkabeginnerscourse.service.KafkaService;
import com.sun.istack.internal.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    private KafkaService service;

    @PostMapping("producer/simple-demo")
    public void producerHelloWorld (@RequestParam("message") @NotNull String message) {
        service.producerSimpleDemo(message);
    }

}
