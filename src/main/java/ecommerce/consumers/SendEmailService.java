package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SendEmailService {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                continue;
            } else {
                System.out.println("Request identified! Quantity " + records.count());
                for(ConsumerRecord<String, String> record: records) {
                    System.out.println("----------------------------------------------");
                    System.out.println("Key::" + record.key());
                    System.out.println("Value::" + record.value());
                    System.out.println("Partition::" + record.partition());
                    System.out.println("Offset::" + record.offset());
                    System.out.println("Topic::" + record.topic());
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Email sent");
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, SendEmailService.class.getSimpleName());
        return properties;
    }

}
