package org.email;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kafka.enterprise.KafkaConsumerService;
import org.main.Email;

import java.util.HashMap;

public class SendEmailService {

    public static void main(String[] args) {
        SendEmailService sendEmailService = new SendEmailService();
        try (KafkaConsumerService consumerService = new KafkaConsumerService(
                "ECOMMERCE_SEND_EMAIL",
                SendEmailService.class.getSimpleName(),
                sendEmailService::parse,
                Email.class,
                new HashMap<>())) {
            consumerService.run();
        }
    }

    public void parse(ConsumerRecord<String, Email> record) {
        System.out.println("Key::" + record.key());
        System.out.println("Value::" + record.value());
        System.out.println("Partition::" + record.partition());
        System.out.println("Offset::" + record.offset());
        System.out.println("Topic::" + record.topic());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println();
        System.out.println("Email sent");
    }

}
