package ecommerce.producers;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(KafkaDispatcher dispatcher = new KafkaDispatcher()) {
            String id = "";
            for(int i = 0; i < 10; i++) {
                id = UUID.randomUUID().toString();
                produceOrder(dispatcher, id);
                produceEmail(dispatcher, id);
            }
        }
    }

    private static void produceOrder(KafkaDispatcher dispatcher, String id) throws ExecutionException, InterruptedException {
        String value = id + ";order 127;user douglas.silva;usd 199.53";
        dispatcher.send("ECOMMERCE_NEW_ORDER", id, value);
    }

    private static void produceEmail(KafkaDispatcher dispatcher, String id) throws ExecutionException, InterruptedException {
        String message = "{" +
                "'id'='"+ id + "', " +
                "subject='ECommerce', " +
                "destination='douglas.silva@email.com.br', " +
                "body='Thank you for purchasing with us! We'll be processing your order shortly!'" +
                "}";
        dispatcher.send("ECOMMERCE_SEND_EMAIL", id, message);

    }

}