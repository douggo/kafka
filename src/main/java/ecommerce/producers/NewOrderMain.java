package ecommerce.producers;

import ecommerce.model.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>()) {
            try(KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>()) {
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
        dispatcher.send("ECOMMERCE_NEW_ORDER", order);
    }

    private static void produceEmail(KafkaDispatcher<String> dispatcher, String id) throws ExecutionException, InterruptedException {
        String message = "{" +
                "'id'='"+ id + "', " +
                "subject='ECommerce', " +
                "destination='douglas.silva@email.com.br', " +
                "body='Thank you for purchasing with us! We'll be processing your order shortly!'" +
                "}";
        dispatcher.send("ECOMMERCE_SEND_EMAIL", message);
    }

}