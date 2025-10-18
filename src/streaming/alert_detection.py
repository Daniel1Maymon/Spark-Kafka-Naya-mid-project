"""
Spark Job: AlertDetection (Streaming Job)

=========================================

Purpose:
--------
This job identifies anomalous driving events in real-time.
It consumes enriched events from the Kafka topic 'samples-enriched',
applies business logic filters to detect anomalies, and writes only
those anomalous events to a new Kafka topic 'alert-data'.

Inputs:
-------
Kafka topic: samples-enriched
Each record is a JSON object representing an enriched car telemetry event:
{
  "event_id": "uuid-xxxx",
  "event_time": "2025-10-17T14:22:35.123Z",
  "car_id": 42,
  "driver_id": 309012654,
  "brand_name": "Toyota",
  "model_name": "Corolla",
  "color_name": "Blue",
  "speed": 137.4,
  "rpm": 6500,
  "gear": 5,
  "expected_gear": 4
}

Outputs:
--------
Kafka topic: alert-data
Each record represents an event that violated at least one rule:
- speed > 120
- rpm > 6000
- gear != expected_gear

Alert Event Schema (JSON):
--------------------------
Same as input (no structural changes).

Process Steps:
--------------
1. Initialize SparkSession (with Kafka configuration)
2. Define input Kafka stream (topic: samples-enriched)
3. Parse JSON messages into structured columns
4. Filter events using alert rules:
      - High speed
      - High RPM
      - Gear mismatch
5. Convert filtered events back to JSON
6. Write filtered events to Kafka (topic: alert-data)
7. Start streaming query and await termination
"""

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import to_json, when, col, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json

# ------------------------------------------------------
# 1. Initialize SparkSession (with Kafka configuration)
# ------------------------------------------------------
spark: SparkSession = (
    SparkSession.builder
    .appName("AlertDetection")

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

print("\nSparkSession created successfully")

# ------------------------------------------------------
# 2. Define input Kafka stream (topic: samples-enriched)
# ------------------------------------------------------
# Create a DataFrame type annotation for clarity
raw_stream_df: DataFrame = (
    # Initialize a streaming read operation from Kafka
    spark.readStream
    # Specify the source format as Kafka
    .format("kafka")
    # Define the Kafka cluster to connect to (list of bootstrap brokers)
    .option("kafka.bootstrap.servers", "kafka:9092")
    # Subscribe to a specific Kafka topic (here: samples-enriched)
    .option("subscribe", "samples-enriched")
    # Set starting offset — "latest" means read only new messages from now on
    # .option("startingOffsets", "latest")
    .option("startingOffsets", "earliest")
    # Trigger the actual creation of the streaming DataFrame
    .load()
)

# Print confirmation to verify successful stream initialization
print(f"Kafka streaming source initialized for topic 'samples-enriched'")

# Inspect the schema of the raw Kafka DataFrame (for debugging/validation)
raw_stream_df.printSchema()

# ------------------------------------------------------
# 3. Parse JSON messages into structured columns
# ------------------------------------------------------
enriched_event_schema: StructType = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
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

# Step 3.2 — Read raw Kafka stream (assumed to be created earlier)
# The DataFrame 'raw_stream_df' contains Kafka records with binary 'value'

# Step 3.3 — Cast Kafka 'value' column from binary to string
string_df = raw_stream_df.selectExpr("CAST(value AS STRING) AS json_str")

# Step 3.4 — Parse the JSON string using the defined schema
parsed_stream_df = string_df.select(
    from_json(col("json_str"), enriched_event_schema).alias("data")
)

# Step 3.5 — Flatten the struct column into individual fields
alerts_candidate_df = parsed_stream_df.select("data.*")

# ------------------------------------------------------
# 4. Filter events using alert rules
# ------------------------------------------------------

# Apply filtering logic to detect anomalies
# We use OR conditions because an event should be flagged if ANY rule is violated.
alerts_df: DataFrame = alerts_candidate_df.filter(
    (col("speed") > 120) |               # Rule 1: Overspeed
    (col("rpm") > 6000) |                # Rule 2: High engine RPM
    (col("gear") != col("expected_gear"))# Rule 3: Gear mismatch
)

# Step 4.3 — Print schema and sample output (for validation)
print("\nFiltered alerts stream (anomalies) schema:")
alerts_df.printSchema()

# ------------------------------------------------------
# 5. Convert filtered events back to JSON
# ------------------------------------------------------
# Create a Struct column containing all relevant fields
# Kafka expects a single column named 'value' with a JSON string per record.
alert_struct_df: DataFrame = (
    alerts_df.select(
        struct(
            "event_id",
            "event_time",
            "car_id",
            "driver_id",
            "brand_name",
            "model_name",
            "color_name",
            "speed",
            "rpm",
            "gear",
            "expected_gear"
        ).alias("data_struct")
    )
)

# Convert the Struct column into a JSON string column named 'value'
alert_json_df: DataFrame = (
    alert_struct_df.select(
        to_json(col("data_struct")).alias("value")
    )
)

# ------------------------------------------------------
# 6. Write filtered events to Kafka (topic: alert-data)
# ------------------------------------------------------
# Define the final DataFrame to write (already contains 'value' as JSON string)
final_kafka_df: DataFrame = alert_json_df

# Define the streaming write query to Kafka
query = (
    final_kafka_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "alert-data")  # Target topic for anomalies
    .option("checkpointLocation", "s3a://spark/checkpoints/alert-data")  # Required for fault tolerance
    .outputMode("append")  # Each filtered event is a new row
    .start()
)

# Print confirmation message
print("\nStreaming query started: writing filtered alert events to Kafka topic 'alert-data'")

# ------------------------------------------------------
# 7. Start streaming query and await termination
# ------------------------------------------------------
# Wait for the query to continue running indefinitely
query.awaitTermination()