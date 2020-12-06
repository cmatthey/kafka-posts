package us.matthey.coco;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

public class Producer {
    public static final String TOPIC = "topic-posts";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9093,localhost:9092");
        prop.put("acks", "all");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        // Synchronously
        for (int i = 0; i < 3; i++) {
            System.out.println(i);
            try {
                ProducerRecord<String, String> rec = new ProducerRecord<>(TOPIC, Integer.toString(i), "test message - " + i);
                kafkaProducer.send(rec).get();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaProducer.close();
            }
        }
        // Asynchronously
        for (int i = 0; i < 3; i++) {
            try {
                ProducerRecord<String, String> rec = new ProducerRecord<>(TOPIC, Integer.toString(i), "test message - " + i);
                kafkaProducer.send(rec, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaProducer.close();
            }
        }
    }

}
