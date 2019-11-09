package trading.system.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;
import trading.system.kafka.SerializableStockData;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RedisStockDataStorage {
    // default kafka topic to read from
    private static final String topicName = "stock-analyzer";

    public static void main(String[] args){
        RedisStockDataStorage dataStorage = new RedisStockDataStorage();
        try{
            dataStorage.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run() {
        Map<String, Object> kafkaParams = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafka-trading");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaParams);
        String host = "192.168.99.100";
        String channel = "average-stock-price";
        Integer port = 6379;
        Jedis jedis = new Jedis(host,port);

        try {
            while (true) {
                try {
                    consumer.subscribe(Collections.singletonList(topicName));
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMinutes(1));
                    System.out.println(String.format("Received new data from kafka %s", consumerRecords.toString()));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        jedis.publish(channel, consumerRecord.value());
                    }
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }  finally {
            consumer.close();
        }
    }

    private static class MyConsumerCallback implements Callback {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("#### received callback [" + metadata + "], exception: [" + exception + "]");
        }
    }
}
