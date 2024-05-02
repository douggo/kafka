package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SendEmailService {

    public static void main(String[] args) {
        SendEmailService sendEmailService = new SendEmailService();
        KafkaConsumerService consumerService = new KafkaConsumerService(
                "ECOMMERCE_SEND_EMAIL_TEST",
                SendEmailService.class.getSimpleName(),
                sendEmailService::parse);
        consumerService.run();
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------------------------");
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
        System.out.println("Email sent");
    }

}
