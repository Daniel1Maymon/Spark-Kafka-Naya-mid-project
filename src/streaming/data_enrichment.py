"""
Spark Job: DataEnrichment (Streaming Job)

=========================================

Purpose:
--------
This job enriches the raw telemetry events produced by the CarsGenerator.
It reads real-time events from Kafka topic 'sensors-sample' and joins them
with static reference tables (cars, car_models, car_colors) stored in S3 (MinIO).
The result is an enriched stream of events containing driver, model, and color
information, as well as a calculated 'expected_gear' based on vehicle speed.
The enriched events are then written to a new Kafka topic 'samples-enriched'.

Inputs:
-------
1. Kafka topic: sensors-sample
   Schema:
   {
     "event_id": "uuid-xxxx",
     "event_time": "2025-10-17T14:22:35.123Z",
     "car_id": 1532495,
     "speed": 97.4,
     "rpm": 4100,
     "gear": 4
   }

2. S3 paths (static lookup tables):
   - s3a://spark/data/dims/cars
       Fields: car_id, driver_id, model_id, color_id
   - s3a://spark/data/dims/car_models
       Fields: model_id, brand_name, model_name
   - s3a://spark/data/dims/car_colors
       Fields: color_id, color_name

Outputs:
--------
- Kafka topic: samples-enriched
  Each enriched event contains driver, brand, model, color, and expected_gear fields.

Enriched Event Schema (JSON):
-----------------------------
{
  "event_id": "uuid-xxxx",
  "event_time": "2025-10-17T14:22:35.123Z",
  "car_id": 1532495,
  "driver_id": 309012654,
  "brand_name": "Toyota",
  "model_name": "Corolla",
  "color_name": "Blue",
  "speed": 97.4,
  "rpm": 4100,
  "gear": 4,
  "expected_gear": 5
}

Process Steps:
--------------
1. Initialize SparkSession (with S3 and Kafka configuration)
2. Define the input Kafka stream (topic: sensors-sample)
3. Parse JSON payloads from Kafka into structured columns
4. Load static dimension tables from S3 (cars, car_models, car_colors)
5. Perform join operations to enrich each event:
      - Join with 'cars' on car_id
      - Join with 'car_models' on model_id
      - Join with 'car_colors' on color_id
6. Derive 'expected_gear' column based on the speed range
7. Write the enriched stream back to Kafka (topic: samples-enriched)
8. Start the streaming query and wait for termination
"""
from pyspark.sql import SparkSession, DataFrame, Row
from kafka import KafkaProducer
import json
from typing import Dict, Any, List
import random
import time
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import to_json, when, col, struct

# ------------------------------------------------------
# 1. Initialize SparkSession (with S3 and Kafka configuration)
# ------------------------------------------------------
spark: SparkSession = (
    SparkSession.builder
    .appName("CreateCars")

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
# 2. Define the input Kafka stream (topic: sensors-sample)
# ------------------------------------------------------

# Create a DataFrame type annotation for clarity
raw_stream_df: DataFrame = (
    # Initialize a streaming read operation from Kafka
    spark.readStream
    # Specify the source format as Kafka
    .format("kafka")
    # Define the Kafka cluster to connect to (list of bootstrap brokers)
    .option("kafka.bootstrap.servers", "kafka:9092")
    # Subscribe to a specific Kafka topic (here: sensors-sample)
    .option("subscribe", "sensors-sample")
    # Set starting offset — "latest" means read only new messages from now on
    # .option("startingOffsets", "latest")
    .option("startingOffsets", "earliest")
    # Trigger the actual creation of the streaming DataFrame
    .load()
)

# Print confirmation to verify successful stream initialization
print(f"Kafka streaming source initialized for topic 'sensors-sample'")

# Inspect the schema of the raw Kafka DataFrame (for debugging/validation)
raw_stream_df.printSchema()


# ------------------------------------------------------
# 3. Parse JSON payloads from Kafka into structured columns
# ------------------------------------------------------

from pyspark.sql.streaming import query
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json

# Define the JSON schema based on the DataGenerator output
event_schema: StructType = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

# Convert the Kafka binary "value" column into a readable string
# (Kafka sends message values as bytes, so we cast it to STRING first)
string_df = raw_stream_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse the JSON string column into structured columns according to the schema
parsed_stream_df = string_df.select(from_json(col("json_str"), event_schema).alias("data"))

# Expand the nested "data" struct into individual top-level columns
events_df = parsed_stream_df.select("data.*")

# Display the schema for validation
events_df.printSchema()

# Optional: show a few records for debugging (in console mode only)
# (This triggers a temporary micro-batch printout if run in debug mode)
# events_df.writeStream.format("console").outputMode("append").start()


# ------------------------------------------------------
# 4. Load static dimension tables from S3 (cars, car_models, car_colors)
# ------------------------------------------------------
# Load cars dimension (contains mapping between car_id, driver_id, model_id, color_id)
cars_df: DataFrame = (
    spark.read
    .format("parquet")
    .option("header", "true")
    .load("s3a://spark/data/dims/cars")
)

# Load car models dimension (brand and model names)
car_models_df: DataFrame = (
    spark.read
    .format("parquet")
    .option("header", "true")
    .load("s3a://spark/data/dims/car_models")
)

# Rename columns to match expected output schema
car_models_df = (
    car_models_df
    .withColumnRenamed("car_brand", "brand_name")
    .withColumnRenamed("car_model", "model_name")
)

# Load car colors dimension (color names)
car_colors_df: DataFrame = (
    spark.read
    .format("parquet")
    .option("header", "true")
    .load("s3a://spark/data/dims/car_colors")
)

# Validate schemas to ensure correct structure
print("\nStatic dimension tables loaded successfully:\n")
cars_df.printSchema()
car_models_df.printSchema()
car_colors_df.printSchema()

# Optional: preview few rows from each table for sanity check
print("\nSample records from cars_df:")
cars_df.show(5, truncate=False)

print("\nSample records from car_models_df:")
car_models_df.show(5, truncate=False)

print("\nSample records from car_colors_df:")
car_colors_df.show(5, truncate=False)


# ------------------------------------------------------
# 5. Perform join operations to enrich each event
# ------------------------------------------------------

# Join streaming events with static reference tables
# Step 1: Join with cars_df on car_id to get driver_id, model_id, color_id
enriched_df_step1: DataFrame = (
    events_df.join(cars_df, on="car_id", how="left")
)

# Step 2: Join with car_models_df on model_id to get brand_name and model_name
enriched_df_step2: DataFrame = (
    enriched_df_step1.join(car_models_df, on="model_id", how="left")
)

# Step 3: Join with car_colors_df on color_id to get color_name
enriched_events_df: DataFrame = (
    enriched_df_step2.join(car_colors_df, on="color_id", how="left")
)

# Validate schema and sample records
print("\nEnriched events schema after joins:\n")
enriched_events_df.printSchema()

# print("\nStreaming preview of enriched records:")
# (
#     enriched_events_df
#     .select(
#         "event_id",
#         "event_time",
#         "car_id",
#         "driver_id",
#         "brand_name",
#         "model_name",
#         "color_name",
#         "speed",
#         "rpm",
#         "gear"
#     )
#     .writeStream
#     .format("console")
#     .outputMode("append")
#     .option("truncate", "false")
#     .start()
# )



# ------------------------------------------------------
# 6. Derive 'expected_gear' column based on the speed range
# ------------------------------------------------------
# Define logic for expected_gear based on vehicle speed
# This is a simple heuristic: one gear every 30 km/h (can be adjusted)
enriched_with_gear_df: DataFrame = (
    enriched_events_df
    .withColumn(
        "expected_gear",
        when(col("speed") < 0, 0)
        .when(col("speed") < 30, 1)
        .when(col("speed") < 60, 2)
        .when(col("speed") < 90, 3)
        .when(col("speed") < 120, 4)
        .when(col("speed") < 150, 5)
        .when(col("speed") < 180, 6)
        .otherwise(7)
    )
)

# preview results
# print("\nEnriched events with derived expected_gear:\n")
# enriched_with_gear_df.select(
#     "event_id",
#     "car_id",
#     "speed",
#     "gear",
#     "expected_gear"
# ).show(10, truncate=False)


# ------------------------------------------------------
# 7. Write the enriched stream back to Kafka (topic: samples-enriched)
# ------------------------------------------------------


# Convert enriched DataFrame into a single JSON column named "value"
# Kafka expects key/value pairs, and both must be binary or string.
kafka_output_df: DataFrame = (
    enriched_with_gear_df
    .select(
        to_json(
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
            )
        ).alias("value")
    )
)

# Write the resulting stream to Kafka topic "samples-enriched"
# Step 1 — create a Struct column that combines all fields
struct_df: DataFrame = (
    enriched_with_gear_df
    .select(
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

# Step 2 — convert that Struct column into a JSON string column
json_df: DataFrame = (
    struct_df
    .select(
        to_json(col("data_struct")).alias("value")
    )
)

# Step 3 — final DataFrame to be written to Kafka
kafka_output_df: DataFrame = json_df

# Write the resulting stream to Kafka topic "samples-enriched"
query = (
    kafka_output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "samples-enriched")
    .option("checkpointLocation", "s3a://spark/checkpoints/samples-enriched")
    .outputMode("append")
    .start()
)

print("\nStreaming query started: writing enriched events to Kafka topic 'samples-enriched'")



# ------------------------------------------------------
# 8. Start the streaming query and wait for termination
# ------------------------------------------------------
# Wait for the query to continue running indefinitely
query.awaitTermination()
