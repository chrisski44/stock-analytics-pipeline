package trading.system.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class SerializableStockData implements Serializable {

    private static final long serialVersionUID = -4281495747887648465L;
    private final Timestamp dateTime;
    private final double open;
    private final double high;
    private final double low;
    private final double close;
    private final double adjustedClose;
    private final long volume;
    private final double dividendAmount;
    private final double splitCoefficient;

    public SerializableStockData(@JsonProperty("dateTime") Timestamp dateTime, @JsonProperty("open") double open,
                                 @JsonProperty("high")  double high, @JsonProperty("low") double low,
                                 @JsonProperty("close") double close,
                                 @JsonProperty("adjustedClose") double adjustedClose,
                                 @JsonProperty("volume")  long volume) {
        this.dateTime = dateTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjustedClose = 0.0D;
        this.volume = volume;
        this.dividendAmount = 0.0D;
        this.splitCoefficient = 0.0D;
    }

    public Timestamp getDateTime() {
        return this.dateTime;
    }

    public double getOpen() {
        return this.open;
    }

    public double getHigh() {
        return this.high;
    }

    public double getLow() {
        return this.low;
    }

    public double getClose() {
        return this.close;
    }

    public double getVolume(){return this.volume;}
}
