package com.github.felipsgg.kafkabeginnerscourse.service;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaManager implements KafkaService {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private Logger logger = LoggerFactory.getLogger(KafkaManager.class);


    @Value("${twitter.apikey}")
    private String consumerKey;

    @Value("${twitter.apikey.secret}")
    private String consumerSecret;

    @Value("${twitter.token}")
    private String token;

    @Value("${twitter.token.secret}")
    private String tokenSecret;


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

    /**
     *
     *
     * Example of execution :
     *      Execution #1
     *          id_0 to partition 1
     *          id_1 to partition 0
     *          id_2 to partition 0
     *          id_3 to partition 2
     *          ...
     *      Execution #2
     *          id_0 to partition 1
     *          id_1 to partition 0
     *          id_2 to partition 0
     *          id_3 to partition 2
     *          ...
     *
     *          (same key ALWAYS are going to the same partition)
     *
     * @param message
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void producerDemoWithKeys(String message) throws ExecutionException, InterruptedException {

        // Create the Producer Properties
        // https://kafka.apache.org/26/documentation.html#producerconfigs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Produce a bunch of records so we can check paritions, offsets, ...
        for (int i=1; i<=10; i++) {

            String topic = "first_topic";
            String value = message + " - Iteration nº: " + i;
            String key = "id_" + i;

             // Create a Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>( topic, key, value );

            logger.info("Key: " + key);

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
            }).get();       // block the send() to make it synchronous - DON'T DO THIS IN PRODUCTION!!

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }


    public void consumerSimpleDemo() {

        // Create the Consumer Properties
        //https://kafka.apache.org/26/documentation.html#consumerconfigs

        String groupId = "my-application";
        String topic = "first_topic";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe the consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));

        // Poll for new data
        // ADVICE: this is a bad practice in programming but is good for this example
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));      // new if Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() +  ", Value: " + record.value());
                logger.info("Partition: " + record.partition() +  ", Offset: " + record.offset());
            }
        }

    }

    public void consumerDemoAssignSeek() {

        // Create the Consumer Properties
        //https://kafka.apache.org/26/documentation.html#consumerconfigs

        String groupId = "my-application-assign-seek";
        String topic = "first_topic";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 9L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);


        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;

        // Poll for new data
        while(numberOfMessagesToRead > numberOfMessagesReadSoFar) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));      // new if Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;

                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    break;
                }
            }
        }

        logger.info("Exiting... ");

    }


    public void realWorldExample() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);


        // 1. Create a Twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection
        client.connect();


        // 2. Create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        // 3. Loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);

                // Send data - Asynchronous
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Something bad happened", exception);
                        }
                    }
                });

            }
        }

        logger.info ("End of application");
    }


    /**
     * Fuente:
     *
     * https://github.com/twitter/hbc
     *
     * @param msgQueue
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {



        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("developer");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    private KafkaProducer<String, String> createKafkaProducer() {

        // Create the Producer Properties
        // https://kafka.apache.org/26/documentation.html#producerconfigs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // Create a Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // kafka 2.0 >= 1.1 so we can
                // keep this as 5. Use 1 otherwise


        // High Throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));    // 32KB batch size


        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;

    }



}
