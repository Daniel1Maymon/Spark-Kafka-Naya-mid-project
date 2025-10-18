"""
Spark Job: DataGenerator (Batch Job)
====================================

Purpose:
--------
This job simulates real-time telemetry events for vehicles.
It reads the list of cars from S3 (MinIO) and generates random
sensor readings (speed, rpm, gear) for each car at regular intervals.
The generated events are sent to a Kafka topic named 'sensors-sample'.

Inputs:
-------
- S3 path: s3a://spark/data/dims/cars
  Fields: car_id, driver_id, model_id, color_id

Outputs:
--------
- Kafka topic: sensors-sample
  Each message represents a single car event.

Event Schema (JSON):
--------------------
{
  "event_id": "uuid-xxxx",
  "event_time": "2025-10-17T14:22:35.123Z",
  "car_id": 1532495,
  "speed": 97.4,
  "rpm": 4100,
  "gear": 4
}

Process Steps:
--------------
1. Initialize SparkSession (with S3 and Kafka configuration)
2. Read the list of cars from S3 (Parquet file)
3. Initialize Kafka producer
4. Generate and send random events in a loop
      - Select a random car
      - Generate random telemetry values
      - Build JSON message and send to Kafka
5. Close Kafka producer and stop Spark session
"""

# ------------------------------------------------------
# 1. Initialize SparkSession (with S3 and Kafka configuration)
# ------------------------------------------------------
# Initialize a SparkSession object — the main entry point to Spark functionality.
from pyspark.sql import SparkSession, DataFrame, Row
from kafka import KafkaProducer
import json
from typing import Dict, Any
import random
import time
import uuid
from datetime import datetime, timezone



spark: SparkSession = (
    SparkSession.builder
    # Give the application a logical name (appears in Spark UI and logs)
    .appName("CreateCars")

    # --- JARs ---
    # Explicitly load external JARs required for S3A (MinIO) and Kafka support.
    # Hadoop AWS libraries handle S3A protocol; Kafka JARs enable Spark ↔ Kafka integration.
    .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar,"
            "/opt/spark/jars/hadoop-common-3.3.4.jar,"
            "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar,"
            "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar,"
            "/opt/spark/jars/kafka-clients-3.4.0.jar")

    # --- General performance ---
    # Enable adaptive query execution — Spark dynamically adjusts number of partitions at runtime.
    .config("spark.sql.adaptive.enabled", "true")
    # Allow Spark to automatically merge small partitions after shuffle.
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Use Kryo serializer for faster, more compact serialization than Java serializer.
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # --- S3A / MinIO configs ---
    # Point Spark’s S3A connector to the MinIO endpoint instead of AWS S3.
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    # Provide MinIO access credentials.
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    # Use path-style access (http://endpoint/bucket/key) — required for MinIO.
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # Explicitly set the filesystem implementation class for S3A.
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # --- Performance tuning for S3A ---
    # Enable “fast upload” mode for parallel multipart uploads to S3-compatible storage.
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    # Use disk buffer for multipart upload (less memory pressure, more stability).
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
    # Maximum number of parallel S3 connections.
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    # Max number of threads handling S3A operations.
    .config("spark.hadoop.fs.s3a.threads.max", "200")
    # Timeout for establishing S3A connections (milliseconds).
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
    # Maximum retry attempts on transient S3 errors.
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    # Maximum keys to fetch per S3 listing request.
    .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
    # Size of each multipart upload chunk (100 MB here).
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
    # Logical HDFS block size used internally for I/O planning (32 MB).
    .config("spark.hadoop.fs.s3a.block.size", "33554432")
    # Socket buffer sizes (read/write) for S3A I/O.
    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
    .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")

    # --- Workaround for MinIO "resource deadlock avoided" bug ---
    # Prevent creation of zero-byte “folder” objects (which trigger 500 errors in MinIO).
    .config("spark.hadoop.fs.s3a.create.folders", "false")

    # Build and start the SparkSession
    .getOrCreate()
)

print("\nSparkSession created successfully")


# ------------------------------------------------------
# 2. Read the list of cars from S3 (Parquet file)
# ------------------------------------------------------

# Define the S3 path for the cars dataset
cars_path: str = "s3a://spark/data/dims/cars"

# Read the Parquet file into a Spark DataFrame
cars_df: DataFrame = spark.read.parquet(cars_path)

# Display a few rows for validation (optional)
cars_df.show(5)

# Collect the list of cars into a local Python list for random sampling
# Each element will be a Row(car_id=..., driver_id=..., model_id=..., color_id=...)
cars_list = cars_df.collect()

print(f"cars_list: {cars_list}")


# ------------------------------------------------------
# 3. Initialize Kafka producer
# ------------------------------------------------------

# Define Kafka connection parameters
bootstrap_servers: str = "kafka:9092"
topic_name: str = "sensors-sample"

# Initialize Kafka producer with JSON serialization
# The value_serializer converts Python dicts to JSON bytes before sending
producer: KafkaProducer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",            # Wait for full commit acknowledgment
    retries=3,             # Retry a few times in case of transient errors
    linger_ms=5            # Small delay to batch messages for efficiency
)

print(f"Kafka producer initialized for topic '{topic_name}' at {bootstrap_servers}")


# ------------------------------------------------------
# 4. Generate and send random events in a loop
# ------------------------------------------------------
# Number of total events to generate
num_events: int = 100

# Delay between each event (in seconds)
sleep_interval: float = 0.5

# Define the valid ranges for telemetry data
speed_range: tuple[int, int] = (0, 180)        # km/h
rpm_range: tuple[int, int] = (500, 7000)       # engine RPM
gear_range: tuple[int, int] = (1, 6)           # gears 1–6

# Iterate and generate events
for i in range(num_events):
    # Select a random car from the list
    car: Row = random.choice(cars_list)

    # Generate random telemetry values
    speed: float = round(random.uniform(*speed_range), 2)
    rpm: int = random.randint(*rpm_range)
    gear: int = random.randint(*gear_range)

    # Create unique event metadata
    event_id: str = str(uuid.uuid4())
    event_time: str = datetime.now(timezone.utc).isoformat()

    # Build event payload (JSON-like dict)
    event: Dict[str, Any] = {
        "event_id": event_id,
        "event_time": event_time,
        "car_id": car["car_id"],
        "speed": speed,
        "rpm": rpm,
        "gear": gear
    }

    # Send event to Kafka topic
    producer.send(topic_name, value=event, key=event_id)
    print(f"Sent event {i+1}/{num_events} -> car_id={car['car_id']} speed={speed} rpm={rpm} gear={gear}")

    # Wait briefly to simulate real-time flow
    time.sleep(sleep_interval)


print("Data generator completed")
# ------------------------------------------------------
# 5. Close Kafka producer and stop Spark session
# ------------------------------------------------------
spark.stop()