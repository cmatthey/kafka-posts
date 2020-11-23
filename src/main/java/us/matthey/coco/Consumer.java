package us.matthey.coco;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import javax.sound.midi.SysexMessage;
import java.time.Duration;
import java.util.*;

import static java.lang.Thread.sleep;

public class Consumer {
    public static final String TOPIC = "topic-posts";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9093");
        properties.put("group.id", "consumer-group");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // List all topics
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        Map<String, List<PartitionInfo>> topicsAsked = kafkaConsumer.listTopics();
        for (String t: topicsAsked.keySet()) {
            System.out.println("Topic " + t);
        }

        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        try {
            int count = 0;
            while (true) {
                try {
//                    ConsumerRecords<String, String> records = kafkaConsumer.poll(0);
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));
                    System.out.println("Record counts " + records.count());
                    for (ConsumerRecord record : records) {
//                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                        System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                    }
                    count++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
