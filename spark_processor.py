from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Initialize Spark Engine with Apache Kafka external package
spark = SparkSession.builder \
    .appName("CryptoStreamingProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Bind to Streaming Source Broker
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_trades") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Define the Structural Payload Mapping from Binance Schema
# 'p' = price, 'q' = quantity, 'E' = epoch timestamp millisecond, 's' = ticker symbol
binance_schema = StructType([
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType())
])

parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), binance_schema).alias("data")) \
    .select("data.*")

# 4. Clean data types and casting transformations
clean_streaming_df = parsed_df.select(
    col("s").alias("Symbol"),
    col("p").cast(DoubleType()).alias("Price"),
    col("q").cast(DoubleType()).alias("Quantity"),
    (col("E") / 1000).cast("timestamp").alias("Timestamp")
)

# 5. Output processed streams directly as atomic mini-batch files
query = clean_streaming_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "./spark_output") \
    .option("checkpointLocation", "./spark_checkpoint") \
    .start()

print("⚡ PySpark Structured Streaming Engine is running...")
query.awaitTermination()