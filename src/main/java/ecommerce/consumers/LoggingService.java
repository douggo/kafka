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
        ArrayList<String> consumerSubscriptionList = new ArrayList<>();
        consumerSubscriptionList.add("ECOMMERCE_NEW_ORDER_TEST");
        consumerSubscriptionList.add("ECOMMERCE_SEND_EMAIL_TEST");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(consumerSubscriptionList);

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.isEmpty()) {
                continue;
            } else {
                System.out.println(records.count() + " were identified for logging...");
                int index = 1;
                for(ConsumerRecord<String, String> record: records) {
                    System.out.println("----------------------------------------------");
                    System.out.println("LOG ("+ index + ")");
                    System.out.println("Topic::" + record.topic());
                    System.out.println("Key::" + record.key());
                    System.out.println("Value::" + record.value());
                    System.out.println("Partition::" + record.partition());
                    System.out.println("Offset::" + record.offset());
                    index++;
                }
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LoggingService.class.getSimpleName());
        return properties;
    }

}
