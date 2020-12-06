package us.matthey.coco;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.sound.midi.SysexMessage;
import java.time.Duration;
import java.util.*;

import static java.lang.Thread.sleep;

public class Consumer {
    public static final String TOPIC = "topic-posts-output";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9093");
        prop.put("group.id", "consumer-group-A");
        prop.put("enable.auto.commit", "false");
        prop.put("auto.commit.interval.ms", "1000"); //Only needed when auto commit is true
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // List all topics
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
        Map<String, List<PartitionInfo>> topicsAsked = kafkaConsumer.listTopics();
        for (String t : topicsAsked.keySet()) {
            System.out.println("Topic " + t);
        }

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                kafkaConsumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });
try {
    int count = 0;
    while (true) {
        try {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10000));
            System.out.println("Record counts " + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
            }
            count++;
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} catch (Exception e) {
    System.out.println(e.getMessage());
} finally {
    try {
        kafkaConsumer.commitSync();
    } finally {
        kafkaConsumer.close();
    }
}
    }
}
