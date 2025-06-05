# Manufacturing Quality Inspection Pipeline
## Redpanda + Apache Iceberg + PyIceberg

A real-time manufacturing quality inspection data pipeline that demonstrates streaming analytics with Redpanda (Kafka-compatible) and Apache Iceberg using PyIceberg for time-travel analytics and ACID transactions.

## Architecture

```
[Quality Inspections] → [Redpanda Topic] → [PyIceberg Writer] → [Iceberg Table] → [Analytics]
                                                                       ↓
                                                              [MinIO S3 Storage]
```

## Features

- **Real-time Streaming**: Quality inspection events streamed through Redpanda
- **ACID Transactions**: Atomic commits to Iceberg tables with PyIceberg
- **Time Travel**: Query historical data states and analyze trends over time
- **Partitioned Storage**: Day-based partitioning for efficient querying
- **Manufacturing Analytics**: Comprehensive quality control analytics
- **Scalable Storage**: MinIO S3-compatible object storage backend

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Required Python packages (see installation below)

### 1. Start Infrastructure

```bash
# Start Redpanda, MinIO, and Spark services
docker-compose up -d

# Verify services are running
docker ps
```

### 2. Install Python Dependencies

```bash
pip install confluent-kafka pyiceberg pandas pyarrow minio
```

### 3. Setup Pipeline

```bash
# Create MinIO bucket and Redpanda topic
python setup_pipeline.py
```

### 4. Generate Sample Data

```bash
# Interactive producer with data type selection
python producer.py

# Or use the alternative producer
python produce.py
```

**Choose data generation mode:**
- **Option 1**: Real-time data (current timestamps)
- **Option 2**: Historical data (3-hour span for time-travel demos)

### 5. Stream to Iceberg

```bash
# Start the PyIceberg writer (in separate terminal)
python true_iceberg_writer.py
```

This will:
- Consume messages from Redpanda
- Transform data to Iceberg format
- Commit atomically to Iceberg tables
- Handle partitioning and schema evolution

### 6. Run Analytics

```bash
# Comprehensive manufacturing analytics
python iceberg_analytics.py
```

## 📊 Analytics Features

The pipeline provides detailed manufacturing insights:

- **Hourly Production Trends**: Throughput, defect rates, environmental conditions
- **Defect Distribution Analysis**: By type, station, and operator
- **Shift Performance Comparison**: First vs second half performance
- **Environmental Correlations**: Temperature, humidity, and line speed impact
- **Executive Summary**: Key findings and recommendations

## Pipeline Components

### Core Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Infrastructure services (Redpanda, MinIO, Spark) |
| `setup_pipeline.py` | Initialize MinIO bucket and Redpanda topic |
| `iceberg_catalog.py` | PyIceberg catalog and table schema configuration |
| `true_iceberg_writer.py` | Streaming consumer that writes to Iceberg tables |
| `producer.py` / `produce.py` | Generate realistic manufacturing inspection data |
| `iceberg_analytics.py` | Comprehensive quality control analytics |
| `check_table.py` | Table management and schema validation utilities |

### Data Schema

The quality inspection events include:

```python
{
    'product_id': 'prod_0001',
    'result': 'OK',  # or 'NG' (No Good)
    'defect_type': 'scratch',  # null if OK
    'confidence': 0.95,
    'timestamp': 1693920000000,
    'inspection_station': 'station_A',
    'operator_id': 'operator_001',
    'shift_id': 'day_shift',
    'batch_number': 'BATCH_1234',
    'line_speed': 50.0,  # units/min
    'temperature': 22.5,  # Celsius
    'humidity': 45.0,    # percentage
    'event_time': datetime  # partitioning field
}
```

## 🔧 Configuration

### Service Ports

- **Redpanda**: `localhost:19092` (Kafka API)
- **MinIO Console**: `localhost:9001` (admin: minioadmin/minioadmin)
- **MinIO API**: `localhost:9000`
- **Spark UI**: `localhost:8080`

### Iceberg Configuration

- **Catalog**: SQLite-based with MinIO S3 backend
- **Partitioning**: Day-based on `event_time` field
- **Format**: Parquet with GZIP compression
- **Warehouse**: `s3://inspection-data/warehouse`

## Sample Analytics Output

```
📊 HOURLY PRODUCTION ANALYSIS:
======================================================================
Hour                 Throughput   Defects    Rate %   Speed    Temp°C   Humid%  
----------------------------------------------------------------------
2025-06-05 14:00     45           7          15.6     52.1     22.3     47.2    
2025-06-05 15:00     52           6          11.5     51.8     22.7     45.8    
2025-06-05 16:00     48           9          18.8     50.2     23.1     48.1    

📈 Trends:
  Throughput: 📈 Increasing (1.5 units/hour)
  Defect Rate: 🔴 Worsening (1.6% per hour)
```

## Use Cases

- **Real-time Quality Monitoring**: Track defect rates as they happen
- **Root Cause Analysis**: Correlate defects with environmental factors
- **Operator Performance**: Compare quality metrics across shifts and operators
- **Process Optimization**: Identify optimal operating conditions
- **Historical Analysis**: Time-travel queries for trend analysis
- **Regulatory Compliance**: Immutable audit trail of quality data

## Data Reliability

- **ACID Transactions**: Guaranteed data consistency with Iceberg
- **Schema Evolution**: Safe schema changes without breaking existing data
- **Time Travel**: Query data as it existed at any point in time
- **Atomic Commits**: Batch processing with rollback capabilities
- **Partition Pruning**: Efficient queries with date-based partitioning

## Troubleshooting

### Check Table Status
```bash
python check_table.py
```

### Verify Services
```bash
# Check Redpanda topics
docker exec redpanda-0 rpk topic list

# Check MinIO buckets
docker exec minio mc ls local/
```

### Reset Pipeline
```bash
# Stop services
docker-compose down -v

# Restart clean
docker-compose up -d
python setup_pipeline.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test your changes with sample data
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues:
- Check the troubleshooting section
- Review Docker Compose logs: `docker-compose logs`
- Verify Python dependencies and versions
- Ensure all services are running: `docker ps`

---

