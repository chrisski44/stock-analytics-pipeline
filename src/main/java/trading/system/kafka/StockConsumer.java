package trading.system.kafka;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import com.redislabs.provider.redis.RedisEndpoint;
import com.redislabs.provider.redis.streaming.RedisStreamingContext;
import org.apache.spark.SparkConf;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StockConsumer implements Serializable {

    public static void main(String[] args){
        StockConsumer stockConsumer = new StockConsumer();
        try{
            stockConsumer.run(args);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private StockConsumer(){

    }

    private void run(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
// Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("KafkaTrading")
                .set("spark.redis.port", "6379")
                // optional redis AUTH password
                .set("spark.redis.host", "localhost");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        RedisConfig redisConfig = new RedisConfig(new RedisEndpoint(conf));
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(conf);

        RedisStreamingContext redisStreamingContext = new RedisStreamingContext(jssc.ssc());
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafka-trading");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("trading");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> temp = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) throws IOException {
                        return new Tuple2<String, String>(record.key(), record.value());
                    }
                });
        temp.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
