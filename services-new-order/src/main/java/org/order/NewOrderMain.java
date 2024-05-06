package org.order;

import org.kafka.enterprise.KafkaDispatcher;
import org.main.Email;
import org.main.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>()) {
            try(KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>()) {
                String id = "";
                for(int i = 0; i < 10; i++) {
                    id = UUID.randomUUID().toString();
                    produceOrder(orderDispatcher, id);
                    produceEmail(emailDispatcher, id);
                }
            }
        }
    }

    private static void produceOrder(KafkaDispatcher<Order> dispatcher, String id) throws ExecutionException, InterruptedException {
        BigDecimal amount = BigDecimal.valueOf(Math.random() * 5000);
        Order order = new Order("douglas.silva", id, amount);
        dispatcher.send("ECOMMERCE_NEW_ORDER", id, order);
    }

    private static void produceEmail(KafkaDispatcher<Email> dispatcher, String id) throws ExecutionException, InterruptedException {
        Email email = new Email(
                "Your order was submitted successfully",
                "douglas.silva@gmail.com",
                "Thank you for purchasing with us! We'll be processing your order shortly!"
        );
        dispatcher.send("ECOMMERCE_SEND_EMAIL", id, email);
    }

}