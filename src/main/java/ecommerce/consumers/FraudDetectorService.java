package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        FraudDetectorService fraudService = new FraudDetectorService();
        try(KafkaConsumerService consumerService = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                FraudDetectorService.class.getSimpleName(),
                fraudService::parse)) {
            consumerService.run();
        }
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------");
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
