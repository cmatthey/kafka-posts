package us.matthey.coco;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class Streams {
    public static final String INPUT_TOPIC = "topic-posts";
    public static final String OUPUT_TOPIC = "topic-posts-output";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka/");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
//        builder.<String, String>stream(INPUT_TOPIC).mapValues(value -> String.valueOf(value.length())).to(OUPUT_TOPIC);
        KTable<String, Long> wordCounts = source
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();
        wordCounts.toStream().foreach((w, c) -> System.out.println("wordCounts" + w + " -> " + c));
        wordCounts.toStream().to(OUPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
//        streams.close();
    }
}
