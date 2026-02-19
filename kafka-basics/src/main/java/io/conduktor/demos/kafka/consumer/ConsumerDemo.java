package io.conduktor.demos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties =  new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create consumer config

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());


        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset","earliest");

        try(KafkaConsumer<String, String> consumer =  new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                });
            }
        }
    }
}
