# stock-analytics-pipeline

Streaming pipeline that gathers stock tick data via an API and broadcasts the information via Kafka. The data is consumed using Spark Streaming and passed to Redis to be shown in the UI, and Cassandra for long term storage. 
