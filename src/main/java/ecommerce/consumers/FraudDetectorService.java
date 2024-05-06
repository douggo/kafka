package ecommerce.consumers;

import ecommerce.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {

    public static void main(String[] args) {
        FraudDetectorService fraudService = new FraudDetectorService();
        try (KafkaConsumerService consumerService = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                FraudDetectorService.class.getSimpleName(),
                fraudService::parse,
                Order.class,
                new HashMap<>())) {
            consumerService.run();
        }
    }

    public void parse(ConsumerRecord<String, Order> record) {
        System.out.println("Key::" + record.key());
        System.out.println("Value::" + record.value());
        System.out.println("Partition::" + record.partition());
        System.out.println("Offset::" + record.offset());
        System.out.println("Topic::" + record.topic());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order was verified successfully");
    }

}
