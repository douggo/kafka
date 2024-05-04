package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Pattern;

public class LoggingService {

    public static void main(String[] args) {
        LoggingService loggingService = new LoggingService();
        try (KafkaConsumerService consumerService = new KafkaConsumerService(
                Pattern.compile("ECOMMERCE.*"),
                loggingService.getClass().getSimpleName(),
                loggingService::parse)) {
            consumerService.run();
        }
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("LOG (" + record.topic() + ")");
        System.out.println("Key::" + record.key());
        System.out.println("Value::" + record.value());
        System.out.println("Partition::" + record.partition());
        System.out.println("Offset::" + record.offset());
    }

}
