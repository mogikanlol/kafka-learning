package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;
import java.util.Scanner;

public class TransactionalProducerApplication {

    public static void main(String[] args) {

        // docker exec -it kafka /bin/bash
        // kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
        // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning --isolation-level=read_committed

        System.out.println("Producer application has started");

        Properties defaultProperties = new Properties();
        defaultProperties.put("bootstrap.servers", "localhost:9092");
        defaultProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        defaultProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Properties transactionalProperties = new Properties();
        transactionalProperties.putAll(defaultProperties);
        transactionalProperties.put("transactional.id", "my-id");
        transactionalProperties.put("enable.idempotence", "true");
        transactionalProperties.put("acks", "all");


        KafkaProducer<String, String> transactionalProducer = new KafkaProducer<>(transactionalProperties);
        transactionalProducer.initTransactions();

        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNextLine()) {
            String input = scanner.nextLine();
            if (input.equals("exit")) {
                transactionalProducer.close();
                break;
            }

            if (input.endsWith("-t")) {
                sendInTransaction(transactionalProducer, input);
            } else {
                sendWithoutTransaction(transactionalProducer, input);
            }
        }
    }

    public static void sendInTransaction(KafkaProducer<String, String> producer,
                                         String input) {
        try {
            producer.beginTransaction();
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-1", "key", input);
            producer.send(record);
            producer.commitTransaction();
        } catch (ProducerFencedException ex) {
            producer.close();
        } catch (KafkaException ex) {
            producer.abortTransaction();
        }
    }

    public static void sendWithoutTransaction(KafkaProducer<String, String> producer,
                                         String input) {
        try {
            producer.beginTransaction();
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-1", "key", input);
            producer.send(record);
            producer.abortTransaction();
        } catch (ProducerFencedException ex) {
            producer.close();
        } catch (KafkaException ex) {
            producer.abortTransaction();
        }
    }

}
