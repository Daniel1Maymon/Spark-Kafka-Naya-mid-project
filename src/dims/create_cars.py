import random
from minio import Minio
from pyspark.sql import SparkSession, DataFrame, Row
from typing import List

# Initialize a SparkSession object — the main entry point to Spark functionality.
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

# Helper function to generate a single random car row
def random_car() -> Row:
    return Row(
        car_id=random.randint(1000000, 9999999),          # unique 7-digit ID
        driver_id=random.randint(100000000, 999999999),   # 9-digit ID (can repeat)
        model_id=random.randint(1, 7),                    # reference to car_models
        color_id=random.randint(1, 7)                     # reference to car_colors
    )
    

# Generate list of 20 random cars
data: List[Row] = [random_car() for _ in range(20)]

print('Raw data:')
for row in data:
    print(f'  {row}')


# Convert list to Spark DataFrame
df: DataFrame = spark.createDataFrame(data)

print('\nDataFrame:')
df.show()


print("--------------------------------")
print("Ensuring MinIO bucket exists")

# --- Ensure MinIO bucket exists ---
minio_client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
bucket_name = 'spark'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"\nCreated bucket '{bucket_name}' in MinIO")
else:
    print(f"\nBucket '{bucket_name}' already exists in MinIO")


# Define output S3A path
output_path: str = "s3a://spark/data/dims/cars"


# --- Configure Spark to write files reliably to S3/MinIO storage ---
# These settings ensure data is properly committed and prevent partial writes

# Use the newer, faster file output committer algorithm (version 2 is more efficient)
# (Improves parallel commit performance and reduces I/O overhead)
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

# Ignore cleanup failures to prevent job failures when temporary files can't be deleted
# (Avoids unnecessary job aborts due to leftover temporary files)
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")

# Force Spark to use the S3A committer factory (critical for enabling staging committer)
# (Without this line, Spark keeps using the old FileOutputCommitter)
spark.conf.set(
    "mapreduce.outputcommitter.factory.scheme.s3a",
    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
)

# Use the staging committer (writes files locally, then uploads them safely to S3)
# (Avoids fake folder creation and prevents MinIO deadlock errors)
spark.conf.set("spark.hadoop.fs.s3a.committer.name", "staging")

# Set the local staging directory where files are temporarily stored before upload
# (Spark writes here first, then commits to MinIO atomically)
spark.conf.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/spark-staging")

# Prevent creation of empty folder structures in S3
# (MinIO may fail on PUT 0-byte "folder" objects with 'resource deadlock avoided')
spark.conf.set("spark.hadoop.fs.s3a.create.folders", "false")

# Prevent retention of directory marker objects in S3
# (Ensures MinIO does not keep 0-byte directory markers)
spark.conf.set("spark.hadoop.fs.s3a.directory.marker.retention", "false")

# Disable magic committer (can cause issues with non-AWS S3 services like MinIO)
# (Magic committer uses hidden marker files that MinIO doesn't fully support)
spark.conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "false")


print(spark.conf.get("mapreduce.outputcommitter.factory.scheme.s3a"))
print(spark.conf.get("spark.hadoop.fs.s3a.committer.name"))
print()

# Write DataFrame as Parquet
df.write.mode("overwrite").parquet(output_path)

print(f"Created {output_path}")

# Stop Spark session
spark.stop()