"""
Spark Job: AlertCounter (Streaming Job)

=========================================

Purpose:
--------
This job continuously aggregates alert events over fixed 15-minute time windows.
It consumes anomalous driving events from the Kafka topic 'alert-data',
groups them by event time windows, and computes key metrics such as
counts per color and maximum observed speed, gear, and rpm.
The aggregated results are printed to the console in real-time for monitoring.

Inputs:
-------
Kafka topic: alert-data
Each record is a JSON object representing an anomalous event:
{
  "event_id": "uuid-xxxx",
  "event_time": "2025-10-17T14:22:35.123Z",
  "car_id": 42,
  "driver_id": 309012654,
  "brand_name": "Toyota",
  "model_name": "Corolla",
  "color_name": "Black",
  "speed": 147.4,
  "rpm": 6700,
  "gear": 4,
  "expected_gear": 5
}

Outputs:
--------
Console Sink (Real-time view)
Each output row represents aggregated statistics for a 15-minute window.

Output Schema:
--------------
| window.start | window.end | num_of_rows | num_of_black | num_of_white | num_of_silver | maximum_speed | maximum_gear | maximum_rpm |

Process Steps:
--------------
1. Initialize SparkSession (with Kafka configuration)
2. Define input Kafka stream (topic: alert-data)
3. Parse JSON messages into structured columns
4. Add watermark on 'event_time' (15 minutes)
5. Group data using a 15-minute time window
6. Compute aggregations:
      - num_of_rows: total count of alerts
      - num_of_black / num_of_white / num_of_silver
      - maximum_speed / maximum_gear / maximum_rpm
7. Write results to console in 'complete' output mode
8. Start streaming query and await termination
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    count, to_timestamp, when, col, window, from_json, max, sum as _sum
)

# ------------------------------------------------------
# 1. Initialize SparkSession (with Kafka configuration)
# ------------------------------------------------------
spark: SparkSession = (
    SparkSession.builder
    .appName("AlertCounter")

    # --- Required JARs for S3A (MinIO) and Kafka ---
    .config(
        "spark.jars",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar,"
        "/opt/spark/jars/hadoop-common-3.3.4.jar,"
        "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar,"
        "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar,"
        "/opt/spark/jars/kafka-clients-3.4.0.jar,"
        "/opt/spark/jars/commons-pool2-2.11.1.jar"
    )

    # --- General performance settings ---
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.adaptive.enabled", "true")

    # --- S3A / MinIO configuration ---
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # --- MinIO stability workaround ---
    .config("spark.hadoop.fs.s3a.create.folders", "false")

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("\nSparkSession created successfully")

# ------------------------------------------------------
# 2. Define input Kafka stream (topic: alert-data)
# ------------------------------------------------------
print("\nInitializing Kafka stream...")
raw_stream_df: DataFrame = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "alert-data")
    .option("startingOffsets", "earliest")
    .load()
)

print("Kafka streaming source initialized for topic 'alert-data'")
raw_stream_df.printSchema()

# ------------------------------------------------------
# 3. Parse JSON messages into structured columns
# ------------------------------------------------------
alert_event_schema: StructType = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),  # incoming as STRING
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("expected_gear", IntegerType(), True)
])

# Step 3.1 — Cast Kafka 'value' column from binary to string.
string_df: DataFrame = raw_stream_df.selectExpr("CAST(value AS STRING) AS json_str")

# Step 3.2 — Parse JSON into a nested struct column 'data'.
parsed_stream_df: DataFrame = string_df.select(
    from_json(col("json_str"), alert_event_schema).alias("data")
)

# Step 3.3 — Flatten to top-level columns.
alerts_df: DataFrame = parsed_stream_df.select("data.*")

print("\nParsed alert stream schema:")
alerts_df.printSchema()

# ------------------------------------------------------
# 3.5. Debug stream to verify parsing and timestamps
# ------------------------------------------------------
# Parse event_time to a proper timestamp with microseconds + timezone.
# IMPORTANT: Use an explicit format that matches values like:
# "2025-10-17T20:06:33.569184+00:00"
alerts_with_timestamp_df: DataFrame = alerts_df.withColumn(
    "event_time_ts",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
)

print("\nDebugging: showing event_time (string) and event_time_ts (timestamp):")
debug_q = (
    alerts_with_timestamp_df
    .select("event_time", "event_time_ts", "speed", "rpm", "gear", "color_name")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 5)
    .start()
)

# Allow some time to observe the debug output before continuing.
import time
print("Waiting ~20 seconds for debug rows to appear (if any)...")
time.sleep(20)
debug_q.stop()

# ------------------------------------------------------
# 4. Add watermark on parsed timestamp column (not on string)
# ------------------------------------------------------
# NOTE: Using 48 hours for troubleshooting late/old events; switch back to "15 minutes" when verified.
alerts_watermarked_df: DataFrame = alerts_with_timestamp_df.withWatermark(
    "event_time_ts", "48 hours"
)
print("\nWatermark added on 'event_time_ts' (48 hours for debugging)")

# ------------------------------------------------------
# 5. Perform windowed aggregation (15-minute window)
# ------------------------------------------------------
windowed_agg_df: DataFrame = (
    alerts_watermarked_df
    .groupBy(window(col("event_time_ts"), "15 minutes"))
    .agg(
        count("*").alias("num_of_rows"),
        _sum(when(col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
        _sum(when(col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
        _sum(when(col("color_name") == "Silver", 1).otherwise(0)).alias("num_of_silver"),
        max("speed").alias("maximum_speed"),
        max("gear").alias("maximum_gear"),
        max("rpm").alias("maximum_rpm")
    )
)

print("\nWindowed aggregation DataFrame created (15-minute window applied):")
windowed_agg_df.printSchema()

# ------------------------------------------------------
# 6. Compute required metrics and rename columns
# ------------------------------------------------------
final_metrics_df: DataFrame = (
    windowed_agg_df
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("num_of_rows"),
        col("num_of_black"),
        col("num_of_white"),
        col("num_of_silver"),
        col("maximum_speed"),
        col("maximum_gear"),
        col("maximum_rpm")
    )
)

print("\nFinal metrics DataFrame prepared with flattened window columns:")
final_metrics_df.printSchema()

# ------------------------------------------------------
# 7. Write aggregated results to console (complete mode)
# ------------------------------------------------------
query = (
    final_metrics_df.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", "s3a://spark/checkpoints/alert-counter")
    .trigger(processingTime="1 minute")
    .start()
)

print("\nStreaming query started: writing 15-minute aggregated results to console")

# ------------------------------------------------------
# 8. Start streaming query and await termination
# ------------------------------------------------------
query.awaitTermination()
