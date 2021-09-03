package com.github.felipsgg.kafkabeginnerscourse.web;

import com.github.felipsgg.kafkabeginnerscourse.service.KafkaService;
import com.sun.istack.internal.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.Positive;
import java.util.concurrent.ExecutionException;

@RestController
@Validated
public class KafkaController {

    @Autowired
    private KafkaService service;

    @PostMapping("producer/simple-demo")
    public void producerSimpleDemo (@RequestParam("message") @Valid @NotNull String message) {
        service.producerSimpleDemo(message);
    }

    @PostMapping("producer/demo-callback")
    public void producerDemoWithCallback(@RequestParam("message") @Valid @NotNull String message,
                                         @RequestParam("iterations") @Valid @NotNull @Positive Integer iteratons) {
        service.producerDemoWithCallback(message, iteratons);
    }

    @PostMapping("producer/demo-keys")
    public void producerDemoWithCallback(@RequestParam("message") @Valid @NotNull String message)
            throws ExecutionException, InterruptedException {
        service.producerDemoWithKeys(message);
    }

}
