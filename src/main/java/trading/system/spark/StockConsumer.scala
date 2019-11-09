package trading.system.spark

import java.util
import java.util.{HashMap, Map}

import com.redislabs.provider.redis.streaming.RedisStreamingContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class StockConsumer {
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "kafka-trading",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> false)
  // Create context with 2 second batch interval

  val sparkConf = new SparkConf().setAppName("StockConsumer").setMaster("local[*]")

  val topics = "trading"
  val topicsSet = topics.split(",").toSet

  val ssc = new StreamingContext(sparkConf, Seconds(30))

  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
  messages.

  // Start the computation
  ssc.start()
  ssc.awaitTermination()

}
