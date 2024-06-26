package org.kafka.enterprise;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    KafkaProducer<String, T> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(this.getProperties());
    }

    public void send(String topic, String id, T value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, T> record = new ProducerRecord<>(topic, id, value);
        producer.send(record, (data, error) -> {
            if (!Objects.isNull(error)) {
                error.printStackTrace();
                return;
            }
            System.out.println("SUCESSO = { Tópico: " + data.topic() + " Timestamp: " + data.timestamp() + " Posição: " + data.offset() + " }");
        }).get();
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        this.producer.close();
    }

}
