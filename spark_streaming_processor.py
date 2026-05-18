from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("EcommerceRealTimeProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest").load()

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

processed_df = parsed_df.withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

purchases_only_df = processed_df.filter(col("action_type") == "purchase")

query = purchases_only_df.writeStream.outputMode("append").format("console").option("truncate", False).start()
query.awaitTermination()