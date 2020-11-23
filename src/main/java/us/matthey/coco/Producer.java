package us.matthey.coco;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

public class Producer {
    public static final String TOPIC = "topic-posts";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9093");
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        try {
            for (int i =3; i<6; i++) {
                System.out.println(i);
                kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, Integer.toString(i), "test message - " + i ));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }

}
