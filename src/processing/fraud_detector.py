from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, stddev, to_timestamp,
    abs as spark_abs, current_timestamp, lit
)
from pyspark.sql.types import *

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType()),
])

def create_spark_session():
    return (SparkSession.builder
        .appName("FraudDetectionPipeline")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate())

def read_kafka_stream(spark):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withWatermark("timestamp", "5 minutes"))

def detect_fraud(df):
    """
    Two fraud signals:
    1. Amount spike: z-score > 3 vs user's rolling average
    2. High velocity: > 5 transactions in 10 minutes
    """

    # Signal 1: Rolling stats per user (60-minute window)
    user_stats = (df
        .groupBy("user_id", window("timestamp", "60 minutes"))
        .agg(avg("amount").alias("avg_amount"),
             stddev("amount").alias("std_amount")))

    enriched = df.join(user_stats, 
                       (col("user_id") == user_stats.user_id) & 
                       (window(col("timestamp"), "60 minutes") == user_stats.window), 
                       "left")

    flagged_amount = enriched.filter(
        (col("std_amount") > 0) &
        (spark_abs(col("amount") - col("avg_amount")) /
         col("std_amount") > 3.0)
    ).withColumn("fraud_reason", lit("amount_spike"))

    # Signal 2: Velocity check (10-minute window)
    velocity = (df
        .groupBy("user_id", window("timestamp", "10 minutes"))
        .agg(count("*").alias("txn_count"))
        .filter(col("txn_count") > 5))

    flagged_velocity = df.join(
        velocity,
        (col("user_id") == velocity.user_id) &
        (window(col("timestamp"), "10 minutes") == velocity.window)
    ).withColumn("fraud_reason", lit("high_velocity"))

    return flagged_amount.union(flagged_velocity).select("transaction_id", "user_id", "amount", "fraud_reason")

def run_pipeline():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_kafka_stream(spark)
    flagged = detect_fraud(raw_stream)

    query = (flagged.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start())

    query.awaitTermination()

if __name__ == "__main__":
    run_pipeline()