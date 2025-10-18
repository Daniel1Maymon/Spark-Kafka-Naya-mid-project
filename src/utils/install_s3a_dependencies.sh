#!/bin/bash
# S3A + MinIO Dependencies Installation Script
# This script installs all required JAR files for Spark S3A integration with MinIO

echo "Installing S3A + MinIO dependencies for Spark..."

# Navigate to Spark jars directory
cd /opt/spark/jars

echo "Downloading Hadoop AWS JAR..."
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

echo "Downloading AWS Java SDK Bundle..."
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar

echo "Downloading Hadoop Common JAR..."
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

echo "Downloading Commons Pool2 (required for Kafka integration)..."
wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar



echo "Setting proper permissions..."
chmod 644 *.jar

echo "Verifying installation..."
ls -lh | grep -E "(hadoop|aws)"

echo ""
echo "S3A + MinIO dependencies installed successfully!"
echo ""
echo "Required JAR files:"
echo "- hadoop-aws-3.3.4.jar (S3A file system implementation)"
echo "- aws-java-sdk-bundle-1.12.661.jar (AWS SDK for all services)"
echo "- hadoop-common-3.3.4.jar (Core Hadoop functionality)"
echo ""
echo "Total size: ~359MB"
echo ""
echo "Next steps:"
echo "1. Configure Spark session with S3A settings"
echo "2. Ensure MinIO is running on http://minio:9000"
echo "3. Create required buckets before writing data"
