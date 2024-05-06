package ecommerce.consumers;

import ecommerce.model.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaConsumerService<T> implements Closeable {

    KafkaConsumer<String, T> consumer;
    ConsumerFunction parse;

    public KafkaConsumerService(String topic, String className, ConsumerFunction parse, Class<T> type, Map<String, String> overrideProperties) {
        this.consumer = new KafkaConsumer<>(this.getProperties(className, type, overrideProperties));
        this.consumer.subscribe(Collections.singletonList(topic));
        this.parse = parse;
    }

    public KafkaConsumerService(Pattern topic, String className, ConsumerFunction parse, Class<T> type, Map<String, String> overrideProperties) {
        this.consumer = new KafkaConsumer<>(this.getProperties(className, type, overrideProperties));
        this.consumer.subscribe(topic);
        this.parse = parse;
    }

    public void run() {
        while(true) {
            ConsumerRecords<String, T> records = this.consumer.poll(Duration.ofMillis(100));
            if(records.isEmpty()) {
                continue;
            } else {
                System.out.println("----------------------------------------------");
                System.out.println("A record was found");
                System.out.println();
                for(ConsumerRecord<String, T> record: records) {
                    this.parse.consume(record);
                }
                System.out.println();
            }
        }
    }

    private Properties getProperties(String className, Class<T> type, Map<String, String> overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, className);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
