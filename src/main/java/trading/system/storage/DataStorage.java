package trading.system.storage;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.patriques.input.timeseries.Interval;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.timeseries.IntraDay;
import org.patriques.output.timeseries.data.StockData;
import trading.system.kafka.SerializableStockData;
import trading.system.kafka.StockConsumer;
import trading.system.kafka.StockProducer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DataStorage {
    // default kafka topic to read from
    private static final String topicName = "stock-analyzer";

    // default cassandra nodes to connect
    private static final String contactPoints = "192.168.99.101";

    // default keyspace to use
    private static final String keySpace = "stock";

    // default table to use
    private static final String dataTable = "stock";

    public static void main(String[] args){
        DataStorage dataStorage = new DataStorage();
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

        Cluster cluster = Cluster.builder()
                .addContactPoints(contactPoints)
                .build();

        Session session = cluster.connect();
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'", keySpace));
        session.close();
        Session stockSession = cluster.connect(keySpace);
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, open_price double, high_price double, low_price double, close_price double, volume double, PRIMARY KEY (stock_symbol,trade_time))", dataTable));

        final Callback callback = new DataStorage.MyConsumerCallback();
        try {
            while (true) {
                try {
                    consumer.subscribe(Collections.singletonList(topicName));
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMinutes(1));
                    for (Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator(); it.hasNext(); ) {
                        ConsumerRecord<String, String> consumerRecord = it.next();
                        try{
                            SerializableStockData result = objectMapper.readValue(consumerRecord.value(), SerializableStockData.class);
                            String symbol = consumerRecord.key();
                            System.out.println("Start to persist data to cassandra for: " + symbol);
                            double high = result.getHigh();
                            double low = result.getLow();
                            double volume = result.getVolume();
                            double close = result.getClose();
                            double open = result.getOpen();
                            String timestamp = result.getDateTime().toString();
                            String statement = String.format("INSERT INTO %s (stock_symbol, trade_time, open_price, high_price," +
                                    "low_price,close_price, volume) VALUES ('%s', '%s', %d, %d, %d, %d, %d)" ,
                                    dataTable, symbol, timestamp, open, high,low,close,volume);
                            stockSession.execute(statement);
                            System.out.println(String.format("Persisted data to cassandra for symbol: %s, price: %f, tradetime: %s",symbol, open, timestamp));
                        } catch(Exception e){
                            System.out.println("failed to push to Cassandra: " + e.getMessage());
                        }
                    }
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }  finally {
            consumer.close();
            stockSession.close();
            cluster.close();
        }
    }

    private static class MyConsumerCallback implements Callback {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("#### received callback [" + metadata + "], exception: [" + exception + "]");
        }
    }
}
