# Spark-Kafka Real-Time Data Pipeline

A comprehensive real-time data processing pipeline built with Apache Spark, Kafka, and MinIO (S3-compatible storage). This project demonstrates a complete streaming data architecture for processing vehicle telemetry data with anomaly detection and real-time analytics.

## Architecture Overview

```
Spark Job: CarsGenerator
(reads cars from S3 → generates random events)
        ↓
Kafka topic: sensors-sample
        ↓
Spark Job: DataEnrichment
(reads from Kafka → joins with S3 tables → adds expected_gear)
        ↓
Kafka topic: samples-enriched
        ↓
Spark Job: AlertDetection
(filters anomalies: speed>120, rpm>6000, gear mismatch)
        ↓
Kafka topic: alert-data
        ↓
Spark Job: AlertCounter
(aggregates alerts every 15 minutes)
        ↓
Console Output
```

## Features

- **Real-time Data Generation**: Simulates vehicle telemetry events with random sensor readings
- **Data Enrichment**: Joins streaming data with static dimension tables (cars, models, colors)
- **Anomaly Detection**: Identifies anomalous driving patterns (high speed, high RPM, gear mismatch)
- **Real-time Analytics**: Aggregates alerts over 15-minute windows with color-based metrics
- **Fault Tolerance**: Uses Kafka checkpoints and S3 storage for reliable data processing
- **Scalable Architecture**: Built with Apache Spark for distributed processing

## Prerequisites

- Docker and Docker Compose
- 8GB+ RAM recommended
- Ports 2181, 4040, 8888, 9015-9017, 9092, 22022 available

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd Spark-Kafka-Naya-mid-project
```

### 2. Start the Infrastructure

```bash
docker-compose up -d
```

This will start:
- **MinIO**: S3-compatible storage (http://localhost:9015)
- **Kafka + Zookeeper**: Message streaming platform
- **Kafdrop**: Kafka UI (http://localhost:9017)
- **Spark Development Environment**: Containerized Spark setup

### 3. Access the Development Environment

```bash
# SSH into the Spark container
ssh developer@localhost -p 22022
# Password: developer

# Or access Jupyter Notebook
# Open http://localhost:8888 in your browser
```

### 4. Run the Pipeline

#### Step 1: Create Dimension Tables
```bash
# Inside the Spark container
cd /home/developer/src/dims
python3 create_colors.py
python3 create_models.py
python3 create_cars.py
```

#### Step 2: Generate Sample Data
```bash
cd /home/developer/src/streaming
python3 data_generator.py
```

#### Step 3: Start Data Enrichment (Terminal 1)
```bash
python3 data_enrichment.py
```

#### Step 4: Start Alert Detection (Terminal 2)
```bash
python3 alert_detection.py
```

#### Step 5: Start Alert Counter (Terminal 3)
```bash
python3 alert_counter.py
```

## Data Flow

### 1. Data Generation (`data_generator.py`)
- **Type**: Batch Job
- **Input**: Static car dimension table from S3
- **Output**: Kafka topic `sensors-sample`
- **Purpose**: Generates random vehicle telemetry events

**Sample Event:**
```json
{
  "event_id": "uuid-xxxx",
  "event_time": "2025-10-17T14:22:35.123Z",
  "car_id": 1532495,
  "speed": 97.4,
  "rpm": 4100,
  "gear": 4
}
```

### 2. Data Enrichment (`data_enrichment.py`)
- **Type**: Streaming Job
- **Input**: Kafka topic `sensors-sample`
- **Output**: Kafka topic `samples-enriched`
- **Purpose**: Enriches raw events with driver, brand, model, color, and expected gear

**Enriched Event:**
```json
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
```

### 3. Alert Detection (`alert_detection.py`)
- **Type**: Streaming Job
- **Input**: Kafka topic `samples-enriched`
- **Output**: Kafka topic `alert-data`
- **Purpose**: Filters events that violate driving rules

**Alert Rules:**
- Speed > 120 km/h
- RPM > 6000
- Gear ≠ Expected Gear

### 4. Alert Counter (`alert_counter.py`)
- **Type**: Streaming Job
- **Input**: Kafka topic `alert-data`
- **Output**: Console (real-time metrics)
- **Purpose**: Aggregates alerts over 15-minute windows

**Output Metrics:**
- Total alert count per window
- Count by color (Black, White, Silver)
- Maximum speed, gear, and RPM values

## Project Structure

```
├── src/
│   ├── dims/                    # Dimension table creation
│   │   ├── create_cars.py      # Generate car-driver mappings
│   │   ├── create_colors.py    # Create color reference table
│   │   └── create_models.py    # Create car model reference table
│   ├── streaming/              # Main streaming jobs
│   │   ├── data_generator.py   # Generate sample telemetry data
│   │   ├── data_enrichment.py  # Enrich events with dimension data
│   │   ├── alert_detection.py  # Detect anomalous driving patterns
│   │   ├── alert_counter.py    # Aggregate alerts over time windows
│   │   └── write_to_kafka.py   # Utility for manual Kafka writes
│   └── utils/                  # Utility scripts
│       ├── smoke_test.py       # Test S3 and Kafka connectivity
│       └── install_s3a_dependencies.sh
├── storage/                    # Local storage for MinIO data
├── notebooks/                  # Jupyter notebooks
├── docker-compose.yaml         # Infrastructure definition
├── Dockerfile.sparkdev         # Spark development environment
└── README.md
```

## Configuration

### Spark Configuration
All Spark jobs are configured with:
- **S3A Integration**: MinIO endpoint and credentials
- **Kafka Integration**: Required JARs and connection settings
- **Performance Tuning**: Adaptive query execution, Kryo serialization
- **Fault Tolerance**: Checkpoint locations in S3

### Kafka Topics
- `sensors-sample`: Raw telemetry events
- `samples-enriched`: Enriched events with dimension data
- `alert-data`: Filtered anomalous events

### MinIO Buckets
- `spark/data/dims/`: Dimension tables (cars, models, colors)
- `spark/checkpoints/`: Spark streaming checkpoints

## Troubleshooting

### Common Issues

1. **MinIO Connection Errors**
   ```bash
   # Check if MinIO is running
   docker ps | grep minio
   
   # Check MinIO logs
   docker logs minio
   ```

2. **Kafka Connection Issues**
   ```bash
   # Check Kafka status
   docker logs kafka
   
   # List topics
   docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

3. **Spark Job Failures**
   ```bash
   # Check Spark UI at http://localhost:4040
   # Review container logs
   docker logs spark-dev
   ```

### Debug Mode
Run the smoke test to verify connectivity:
```bash
python3 src/utils/smoke_test.py
```

## Monitoring

- **Spark UI**: http://localhost:4040
- **Kafka UI (Kafdrop)**: http://localhost:9017
- **MinIO Console**: http://localhost:9016
- **Jupyter Notebooks**: http://localhost:8888

## Development Workflow

1. **Modify Code**: Edit Python files in `src/`
2. **Test Changes**: Use smoke test or run individual components
3. **Debug**: Check logs and monitoring UIs
4. **Deploy**: Restart affected containers if needed

## Notes

- The pipeline is designed for development and testing
- All data is stored locally in the `storage/` directory
- Checkpoints ensure fault tolerance for streaming jobs
- The system uses ARM64 architecture (adjust Docker images if needed)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is for educational purposes. Please ensure compliance with Apache Spark, Kafka, and other component licenses.
