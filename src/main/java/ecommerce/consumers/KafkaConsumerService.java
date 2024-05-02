package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumerService implements Closeable {

    KafkaConsumer<String,String> consumer;
    ConsumerFunction parse;

    public KafkaConsumerService(String topic, String className, ConsumerFunction parse) {
        this.consumer = new KafkaConsumer<>(this.getProperties(className));
        this.consumer.subscribe(Collections.singletonList(topic));
        this.parse = parse;
    }

    public void run() {
        while(true) {
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
            if(records.isEmpty()) {
                continue;
            } else {
                System.out.println(records.count() + " records found");
                for(ConsumerRecord<String, String> record: records) {
                    this.parse.consume(record);
                }
            }
        }
    }

    private Properties getProperties(String className) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, className);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}