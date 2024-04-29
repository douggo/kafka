package ecommerce.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());
        produceOrder(producer);
        produceEmail(producer);
    }

    private static void produceOrder(KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        for(int i = 0; i < 10; i++) {
            String id = UUID.randomUUID().toString();
            String value = id + ";order 127;user douglas.silva;usd 199.53";
            ProducerRecord<String, String> record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER_TEST", id, value);
            producer.send(record, (data, error) -> {
                if (!Objects.isNull(error)) {
                    error.printStackTrace();
                    return;
                }
                System.out.println("SUCESSO = { Tópico: " + data.topic() + " Timestamp: " + data.timestamp() + " Posição: " + data.offset() + " }");
            }).get();
        }
    }

    private static void produceEmail(KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        String message = "{" +
                "'id'='3', " +
                "subject='ECommerce', " +
                "destination='douglas.silva@email.com.br', " +
                "body='Thank you for purchasing with us! We'll be processing your order shortly!'" +
                "}";
        ProducerRecord<String, String> record = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL_TEST", message, message);
        producer.send(record, (data, error) -> {
            if (!Objects.isNull(error)) {
                error.printStackTrace();
                return;
            }
            System.out.println("SUCESSO = { Tópico: " + data.topic() + " Timestamp: " + data.timestamp() + " Posição: " + data.offset() + " }");
        }).get();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}