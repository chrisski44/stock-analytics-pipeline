package trading.system.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.patriques.AlphaVantageConnector;
import org.patriques.BatchStockQuotes;

import org.patriques.TimeSeries;
import org.patriques.input.timeseries.Interval;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.quote.BatchStockQuotesResponse;
import org.patriques.output.quote.data.StockQuote;
import org.patriques.output.timeseries.IntraDay;
import org.patriques.output.timeseries.data.StockData;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class StockProducer {

    public static void main(String[] args){
        StockProducer producer = new StockProducer();
        producer.run(args);
    }

    public StockProducer() {
    }

    private void run(String[] args) {
        String brokers = "127.0.0.1:9092";
        String topic = "trading";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        ObjectMapper objectMapper = new ObjectMapper();

        List<String> symbols = Arrays.asList("INTC", "BABA", "TSLA", "GOOG", "AMZN", "NFLX");
        String apiKey = "6OM26BY8DXJBCFR4";
        int timeout = 3000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries timeSeries = new TimeSeries(apiConnector);
        final Callback callback = new MyProducerCallback();
        try {
            while (true) {
                try {
                    String symbol = "MSFT";
                    IntraDay response = timeSeries.intraDay(symbol, Interval.ONE_MIN, OutputSize.COMPACT);
                    Map<String, String> metaData = response.getMetaData();

                    List<StockData> stockData = response.getStockData();
                    stockData.forEach(stock -> {
                        SerializableStockData data = new SerializableStockData(Timestamp.valueOf(stock.getDateTime()),
                                stock.getOpen(), stock.getHigh(), stock.getLow(), stock.getClose(), stock.getAdjustedClose(), stock.getVolume());
                        try {
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,
                                    metaData.get("2. Symbol"), objectMapper.writeValueAsString(data));
                            System.out.println(objectMapper.writeValueAsString(data));
//                            producer.send(producerRecord, callback);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    });
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }  finally {
            producer.flush();
            producer.close(5, TimeUnit.SECONDS);
        }
    }

    public List<StockQuote> getStocks(List<String> stockList, final BatchStockQuotes batchStockQuotes) {
        List<StockQuote> stockQuoteList = new ArrayList<StockQuote>();

        try {
            BatchStockQuotesResponse response = batchStockQuotes.quote(stockList.toArray(new String[stockList.size()]));
            return response.getStockQuotes();
        } catch (Exception e) {
            System.out.println("Unable to retriever information for : " + stockList);
        }
        return stockQuoteList;
    }

    private static class MyProducerCallback implements Callback {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            System.out.println("#### received callback [" + metadata + "], exception: [" + exception + "]");
        }

    }
}
