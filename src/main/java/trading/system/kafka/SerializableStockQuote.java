package trading.system.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDateTime;

public class SerializableStockQuote implements Serializable {
    private String symbol;
    private double price;
    private long volume;
    private String localDateTime;

    public SerializableStockQuote(@JsonProperty("symbol") String symbol, @JsonProperty("price") double price,
                                  @JsonProperty("volume")long volume,
                                  @JsonProperty("localDateTime") String localDateTime) {
        this.symbol = symbol;
        this.price = price;
        this.volume = volume;
        this.localDateTime = localDateTime;
    }

    public String getSymbol() {
        return this.symbol;
    }

    public void setSymbol(final String symbol){
        this.symbol = symbol;
    }

    public void setPrice(final double price){
        this.price = price;
    }

    public void setVolume(final long volume){
        this.volume = volume;
    }

    public void setTLocalDateTime(final String localDateTime){
        this.localDateTime = localDateTime;
    }
    public double getPrice() {
        return this.price;
    }

    public long getVolume() {
        return this.volume;
    }

    public String getLocalDateTime () {
        return this.localDateTime;
    }
}
