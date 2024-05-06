package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LoggingService {

    public static void main(String[] args) {
        LoggingService loggingService = new LoggingService();

        HashMap<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumerService consumerService = new KafkaConsumerService(
                Pattern.compile("ECOMMERCE.*"),
                loggingService.getClass().getSimpleName(),
                loggingService::parse,
                String.class,
                overrideProperties)) {
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
