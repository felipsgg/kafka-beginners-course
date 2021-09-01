package com.github.felipsgg.kafkabeginnerscourse.service;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class KafkaManager implements KafkaService {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private Logger logger = LoggerFactory.getLogger(KafkaManager.class);

    /**
     * Producer simple example. Send a string (message) to a kafka topic called first_topic
     * @param message
     */
    public void producerSimpleDemo(String message) {

        // Create the Producer Properties
        // https://kafka.apache.org/26/documentation.html#producerconfigs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create a Producer Record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", message);

        // Send data - Asynchronous
        producer.send(record);

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }


    public void producerDemoWithCallback(String message, Integer iterations) {

        // Create the Producer Properties
        // https://kafka.apache.org/26/documentation.html#producerconfigs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Produce a bunch of records so we can check paritions, offsets, ...
        for (int i=1; i<=iterations; i++) {

            // Create a Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", message + " - Iteration nº: " + i);

            // Send data - Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    // Executes everytime a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }

}
