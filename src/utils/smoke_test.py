from pyspark.sql import SparkSession
from minio import Minio

# --- Create Spark session with S3 (MinIO) + Kafka configs ---
spark = (
    SparkSession.builder
    .appName("SmokeTest")
    # --- JARs ---
    .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar,"
            "/opt/spark/jars/hadoop-common-3.3.4.jar,"
            "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar,"
            "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar,"
            "/opt/spark/jars/kafka-clients-3.4.0.jar")
    # --- General performance ---
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # --- S3A / MinIO configs ---
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # --- Performance tuning for S3A ---
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .config("spark.hadoop.fs.s3a.threads.max", "200")
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
    .config("spark.hadoop.fs.s3a.block.size", "33554432")       # 32MB
    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
    .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
    .getOrCreate()
)


print("\nSparkSession created successfully")

# --- Ensure MinIO bucket exists ---
minio_client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
bucket_name = 'spark'
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"\nCreated bucket '{bucket_name}' in MinIO")
else:
    print(f"\nBucket '{bucket_name}' already exists in MinIO")

# --- Create simple DataFrame ---
data = [("Mazda", "3"), ("Toyota", "Corolla"), ("Kia", "Sportage")]
df = spark.createDataFrame(data, ["brand", "model"])

print("\nDataFrame created:")
df.show()

# --- Try writing to MinIO (S3A path) ---
output_path = "s3a://spark/test-output"
df.write.mode("overwrite").parquet(output_path)
print(f"\nData written successfully to {output_path}")

# --- Try reading it back ---
read_df = spark.read.parquet(output_path)
print("\nData read successfully from MinIO:")
read_df.show()

# --- Kafka connectivity check ---
# This part checks that Spark recognizes Kafka connector
# Try to create a Kafka streaming DataFrame to test Kafka connectivity
try:
    # Create a streaming DataFrame from Kafka
    kafka_df = (
        # Use Spark's structured streaming API
        spark.readStream
        # Specify Kafka as the data source format
        .format("kafka")
        # Configure Kafka broker connection (kafka container on port 9092)
        .option("kafka.bootstrap.servers", "kafka:9092")
        # Subscribe to the 'sensors-sample' topic
        .option("subscribe", "sensors-sample")
        # Load the streaming source (this creates the DataFrame)
        .load()
    )
    print("\nKafka connector is available and topic 'sensors-sample' is accessible (if exists).")
except Exception as e:
    print("\nKafka connector test failed (topic may not exist yet):", e)

spark.stop()
print("\nSpark session stopped successfully")
