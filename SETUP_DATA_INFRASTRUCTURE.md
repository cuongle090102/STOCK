# Vietnamese Algorithmic Trading System - Data Infrastructure Setup

This guide provides step-by-step instructions to set up the complete data infrastructure for the Vietnamese Algorithmic Trading System, including data sources, Kafka, PySpark, and Delta Lake.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Python Environment Setup](#python-environment-setup)
3. [Apache Kafka Setup](#apache-kafka-setup)
4. [Apache Spark & Delta Lake Setup](#apache-spark--delta-lake-setup)
5. [Data Sources Configuration](#data-sources-configuration)
6. [Database Setup (PostgreSQL)](#database-setup-postgresql)
7. [MinIO Object Storage Setup](#minio-object-storage-setup)
8. [Environment Configuration](#environment-configuration)
9. [Testing the Setup](#testing-the-setup)
10. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements
- **Operating System**: Windows 10/11, macOS, or Linux
- **RAM**: Minimum 8GB, Recommended 16GB+
- **Storage**: 50GB+ free space
- **Java**: JDK 11 or higher
- **Python**: 3.9+ (3.11 recommended)
- **Docker**: Latest version (for containerized services)

### Required Software
- Git for version control
- Docker Desktop
- Java Development Kit (JDK) 11+
- Python 3.9+
- Apache Maven (for Spark dependencies)

## Python Environment Setup

### 1. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

### 2. Install Python Dependencies
```bash
# Upgrade pip
pip install --upgrade pip

# Install core dependencies
pip install -r requirements.txt
```

### 3. Create requirements.txt (if not exists)
```bash
# Create requirements.txt with all necessary packages
cat > requirements.txt << EOF
# Core data processing
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=12.0.0

# Apache Spark and Delta Lake
pyspark>=3.4.0
delta-spark>=2.4.0

# Kafka integration
kafka-python>=2.0.2
confluent-kafka>=2.1.1

# Database connections
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0

# API clients and HTTP
requests>=2.31.0
aiohttp>=3.8.0
websockets>=11.0.0

# Data validation and quality
pydantic>=2.0.0
jsonschema>=4.17.0

# Logging and monitoring
loguru>=0.7.0
structlog>=23.1.0

# Configuration management
pyyaml>=6.0
python-dotenv>=1.0.0

# Financial data libraries
yfinance>=0.2.0
ta>=0.10.2

# Testing
pytest>=7.0.0
pytest-asyncio>=0.21.0

# Development tools
black>=23.0.0
ruff>=0.0.272
mypy>=1.4.0

# Jupyter notebook support
jupyter>=1.0.0
jupyterlab>=4.0.0
EOF

# Install all packages
pip install -r requirements.txt
```

## Apache Kafka Setup

### Option 1: Docker Setup (Recommended)

#### 1. Create Docker Compose File
```yaml
# Create docker-compose.kafka.yml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9997:9997"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9997
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    volumes:
      - kafka-data:/var/lib/kafka/data

  schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
```

#### 2. Start Kafka Services
```bash
# Start Kafka cluster
docker-compose -f docker-compose.kafka.yml up -d

# Verify services are running
docker-compose -f docker-compose.kafka.yml ps

# View logs
docker-compose -f docker-compose.kafka.yml logs -f kafka
```

#### 3. Create Vietnamese Market Topics
```bash
# Create topics for Vietnamese market data
docker exec -it kafka kafka-topics --create --topic vn-market-ticks --bootstrap-server localhost:9092 --partitions 12 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic vn-market-bars --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic vn-trading-signals --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic vn-risk-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic vn-market-indices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics to verify
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Option 2: Native Installation

#### For Windows:
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/2.13-3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0

# Start Zookeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# Start Kafka server (in another terminal)
bin\windows\kafka-server-start.bat config\server.properties
```

#### For macOS/Linux:
```bash
# Install using Homebrew (macOS)
brew install kafka

# Or download manually
wget https://downloads.apache.org/kafka/2.13-3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka server (in another terminal)
bin/kafka-server-start.sh config/server.properties
```

## Apache Spark & Delta Lake Setup

### 1. Download and Install Spark
```bash
# Download Spark 3.4.0 with Hadoop 3.3
wget https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
tar -xzf spark-3.4.0-bin-hadoop3.tgz
sudo mv spark-3.4.0-bin-hadoop3 /opt/spark
```

### 2. Set Environment Variables
```bash
# Add to ~/.bashrc (Linux/macOS) or Environment Variables (Windows)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# For Windows, set in System Environment Variables:
# SPARK_HOME=C:\spark\spark-3.4.0-bin-hadoop3
# Add %SPARK_HOME%\bin to PATH
```

### 3. Download Delta Lake JAR
```bash
# Download Delta Lake JAR for Spark 3.4
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar
```

### 4. Configure Spark for Delta Lake
```bash
# Create spark-defaults.conf
cat > $SPARK_HOME/conf/spark-defaults.conf << EOF
spark.sql.extensions                     io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog          org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.adaptive.enabled               true
spark.sql.adaptive.coalescePartitions.enabled true
spark.serializer                         org.apache.spark.serializer.KryoSerializer
spark.sql.streaming.metricsEnabled       true
spark.databricks.delta.retentionDurationCheck.enabled false
spark.databricks.delta.vacuum.parallelDelete.enabled true
EOF
```

### 5. Test Spark Installation
```python
# Create test_spark.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configure Spark with Delta Lake
builder = SparkSession.builder \
    .appName("Delta Lake Test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Test Delta Lake
data = [("VIC", 90000), ("VNM", 65000)]
df = spark.createDataFrame(data, ["symbol", "price"])

# Write as Delta table
df.write.format("delta").mode("overwrite").save("./test_delta_table")

# Read Delta table
delta_df = spark.read.format("delta").load("./test_delta_table")
delta_df.show()

print("✅ Spark and Delta Lake setup successful!")
spark.stop()
```

```bash
# Run test
python test_spark.py
```

## Data Sources Configuration

### 1. Vietnamese Market Data APIs

#### VNDirect API Configuration
```bash
# Create .env file for API credentials
cat > .env << EOF
# VNDirect API Configuration
VNDIRECT_API_URL=https://api.vndirect.com.vn
VNDIRECT_API_KEY=your_api_key_here
VNDIRECT_API_SECRET=your_api_secret_here
VNDIRECT_RATE_LIMIT=60  # requests per minute

# SSI FastConnect API Configuration
SSI_API_URL=https://fc-data.ssi.com.vn
SSI_CONSUMER_ID=your_consumer_id_here
SSI_CONSUMER_SECRET=your_consumer_secret_here
SSI_WEBSOCKET_URL=wss://fc-data.ssi.com.vn/realtime

# TCBS API Configuration
TCBS_API_URL=https://apipubaws.tcbs.com.vn
TCBS_RATE_LIMIT=100  # requests per minute

# Market Data Configuration
MARKET_DATA_BATCH_SIZE=1000
MARKET_DATA_TIMEOUT=30
MARKET_DATA_RETRY_ATTEMPTS=3
EOF
```

#### Update Configuration Files
```yaml
# Update configs/kafka_config.yaml with your settings
kafka:
  bootstrap_servers:
    - "localhost:9092"  # Change if using different host
  
  producer:
    client_id: "vn-algo-producer"
    acks: "all"
    batch_size: 16384
    compression_type: "snappy"

# Update configs/data_sources.yaml
data_sources:
  vndirect:
    enabled: true
    priority: 1
    rate_limit: 60
    timeout: 30
    
  ssi:
    enabled: true
    priority: 2
    websocket_enabled: true
    heartbeat_interval: 30
    
  tcbs:
    enabled: true
    priority: 3
    rate_limit: 100
```

### 2. Market Data Symbols Configuration
```python
# Update src/common/market_symbols.py
VN30_SYMBOLS = [
    "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
    "VHM", "TCB", "MWG", "VRE", "SAB", "NVL", "POW", "KDH", "TPB", "SSI",
    "VPB", "PDR", "STB", "HDB", "MBB", "ACB", "VJC", "VND", "GEX", "DGC"
]

EXCHANGE_MAPPING = {
    "HOSE": VN30_SYMBOLS[:20],  # Most VN30 stocks trade on HOSE
    "HNX": ["ACB", "VND", "SHB", "PVS"],
    "UPCOM": ["GEX", "DGC", "OPC"]
}
```

## Database Setup (PostgreSQL)

### 1. Docker Setup (Recommended)
```yaml
# Add to docker-compose.yml
  postgres:
    image: postgres:15
    container_name: postgres-vnalgo
    environment:
      POSTGRES_DB: vnalgo
      POSTGRES_USER: vnalgo
      POSTGRES_PASSWORD: vnalgo123
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U vnalgo"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  postgres-data:
```

### 2. Initialize Database Schema
```sql
-- Create sql/init.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS strategies;
CREATE SCHEMA IF NOT EXISTS risk_management;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Market data tables
CREATE TABLE market_data.symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    sector VARCHAR(50),
    is_vn30 BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE market_data.daily_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(15,2),
    high_price DECIMAL(15,2),
    low_price DECIMAL(15,2),
    close_price DECIMAL(15,2),
    volume BIGINT,
    value DECIMAL(20,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Insert VN30 symbols
INSERT INTO market_data.symbols (symbol, name, exchange, is_vn30) VALUES
('VIC', 'Vingroup Joint Stock Company', 'HOSE', true),
('VNM', 'Vietnam Dairy Products Joint Stock Company', 'HOSE', true),
('HPG', 'Hoa Phat Group Joint Stock Company', 'HOSE', true),
('VCB', 'Joint Stock Commercial Bank for Foreign Trade of Vietnam', 'HOSE', true),
('FPT', 'FPT Corporation', 'HOSE', true);
-- Add more symbols as needed

-- Create indexes
CREATE INDEX idx_daily_prices_symbol_date ON market_data.daily_prices(symbol, date);
CREATE INDEX idx_daily_prices_date ON market_data.daily_prices(date);
```

### 3. Database Connection Configuration
```python
# Update src/database/connection.py
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'vnalgo',
    'user': 'vnalgo',
    'password': 'vnalgo123',
    'pool_size': 10,
    'max_overflow': 20
}
```

## MinIO Object Storage Setup

### 1. Docker Setup
```yaml
# Add to docker-compose.yml
  minio:
    image: minio/minio:latest
    container_name: minio-vnalgo
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

volumes:
  minio-data:
```

### 2. Create MinIO Buckets
```python
# Create setup_minio.py
from minio import Minio
from minio.error import S3Error

def setup_minio_buckets():
    # Initialize MinIO client
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
    
    # Create buckets for data lake layers
    buckets = [
        "bronze-market-data",
        "silver-market-data", 
        "gold-analytics-data",
        "backups",
        "exports"
    ]
    
    for bucket in buckets:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"✅ Created bucket: {bucket}")
            else:
                print(f"✅ Bucket already exists: {bucket}")
        except S3Error as e:
            print(f"❌ Error creating bucket {bucket}: {e}")

if __name__ == "__main__":
    setup_minio_buckets()
```

```bash
# Install MinIO client and run setup
pip install minio
python setup_minio.py
```

## Environment Configuration

### 1. Complete .env File
```bash
# Create comprehensive .env file
cat > .env << EOF
# Environment
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=INFO

# Database Configuration
DATABASE_URL=postgresql://vnalgo:vnalgo123@localhost:5432/vnalgo
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_CONSUMER_GROUP_ID=vn-algo-consumers
KAFKA_AUTO_OFFSET_RESET=latest

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_APP_NAME=VietnameseAlgoTrading
SPARK_SQL_WAREHOUSE_DIR=./spark-warehouse
DELTA_LAKE_PATH=./data/delta

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

# Vietnamese Market Data APIs
VNDIRECT_API_URL=https://api.vndirect.com.vn
VNDIRECT_API_KEY=your_api_key_here
VNDIRECT_API_SECRET=your_api_secret_here
VNDIRECT_RATE_LIMIT=60

SSI_API_URL=https://fc-data.ssi.com.vn
SSI_CONSUMER_ID=your_consumer_id_here
SSI_CONSUMER_SECRET=your_consumer_secret_here
SSI_WEBSOCKET_URL=wss://fc-data.ssi.com.vn/realtime

TCBS_API_URL=https://apipubaws.tcbs.com.vn
TCBS_RATE_LIMIT=100

# Market Data Configuration
MARKET_TIMEZONE=Asia/Ho_Chi_Minh
TRADING_HOURS_MORNING_START=09:00
TRADING_HOURS_MORNING_END=11:30
TRADING_HOURS_AFTERNOON_START=13:00
TRADING_HOURS_AFTERNOON_END=15:00

# Data Processing Configuration
BATCH_SIZE=1000
PROCESSING_TIMEOUT=300
MAX_RETRY_ATTEMPTS=3
DATA_QUALITY_THRESHOLD=0.8

# Monitoring and Alerting
GRAFANA_URL=http://localhost:3000
GRAFANA_USERNAME=admin
GRAFANA_PASSWORD=admin123

# Jupyter Configuration
JUPYTER_PORT=8888
JUPYTER_TOKEN=vnalgo123
EOF
```

### 2. Configuration Validation Script
```python
# Create validate_config.py
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def validate_configuration():
    """Validate all configuration settings."""
    print("🔍 Validating Configuration...")
    
    # Load environment variables
    load_dotenv()
    
    required_vars = [
        'DATABASE_URL',
        'KAFKA_BOOTSTRAP_SERVERS',
        'SPARK_MASTER',
        'MINIO_ENDPOINT',
        'VNDIRECT_API_URL',
        'SSI_API_URL',
        'TCBS_API_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        return False
    
    # Check directory structure
    required_dirs = [
        'data/delta/bronze',
        'data/delta/silver', 
        'data/delta/gold',
        'data/samples',
        'configs',
        'logs'
    ]
    
    for dir_path in required_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Directory ready: {dir_path}")
    
    print("✅ Configuration validation successful!")
    return True

if __name__ == "__main__":
    success = validate_configuration()
    sys.exit(0 if success else 1)
```

## Testing the Setup

### 1. Complete Integration Test
```python
# Create test_integration.py
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

async def test_full_integration():
    """Test complete data infrastructure integration."""
    print("🧪 Starting Full Integration Test...")
    
    tests_passed = []
    tests_failed = []
    
    # Test 1: Database Connection
    try:
        from src.database.connection import test_connection
        if test_connection():
            tests_passed.append("Database Connection")
        else:
            tests_failed.append("Database Connection")
    except Exception as e:
        tests_failed.append(f"Database Connection: {e}")
    
    # Test 2: Kafka Connection
    try:
        from kafka import KafkaProducer, KafkaConsumer
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.close()
        tests_passed.append("Kafka Connection")
    except Exception as e:
        tests_failed.append(f"Kafka Connection: {e}")
    
    # Test 3: Spark and Delta Lake
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder.appName("Integration Test")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.stop()
        tests_passed.append("Spark & Delta Lake")
    except Exception as e:
        tests_failed.append(f"Spark & Delta Lake: {e}")
    
    # Test 4: MinIO Connection
    try:
        from minio import Minio
        client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin123", secure=False)
        client.list_buckets()
        tests_passed.append("MinIO Connection")
    except Exception as e:
        tests_failed.append(f"MinIO Connection: {e}")
    
    # Test 5: Data Sources (Mock)
    try:
        from src.ingestion.vnd_direct import VNDDirectClient
        # This will test the class structure without actual API calls
        client = VNDDirectClient()
        tests_passed.append("Data Sources Structure")
    except Exception as e:
        tests_failed.append(f"Data Sources Structure: {e}")
    
    # Print results
    print(f"\n📊 Integration Test Results:")
    print(f"✅ Passed: {len(tests_passed)} tests")
    for test in tests_passed:
        print(f"   ✓ {test}")
    
    if tests_failed:
        print(f"❌ Failed: {len(tests_failed)} tests")
        for test in tests_failed:
            print(f"   ✗ {test}")
        return False
    
    print(f"\n🎉 All integration tests passed!")
    return True

if __name__ == "__main__":
    success = asyncio.run(test_full_integration())
    sys.exit(0 if success else 1)
```

### 2. Run All Tests
```bash
# Start all services
docker-compose up -d

# Wait for services to be ready
sleep 30

# Validate configuration
python validate_config.py

# Run integration tests
python test_integration.py

# Test individual components
python scripts/demo_delta_lake_final.py
python scripts/final_kafka_demo.py
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Kafka Connection Issues
```bash
# Check if Kafka is running
docker ps | grep kafka

# Check Kafka logs
docker logs kafka

# Test Kafka connection
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

#### 2. Spark/Delta Lake Issues
```bash
# Check Java version (must be 11+)
java -version

# Check Spark installation
spark-submit --version

# Test PySpark
python -c "from pyspark.sql import SparkSession; print('PySpark works!')"
```

#### 3. Database Connection Issues
```bash
# Check PostgreSQL status
docker logs postgres-vnalgo

# Test connection
psql postgresql://vnalgo:vnalgo123@localhost:5432/vnalgo -c "SELECT version();"
```

#### 4. MinIO Issues
```bash
# Check MinIO status
docker logs minio-vnalgo

# Access MinIO console
# Open http://localhost:9001 in browser
# Login: minioadmin / minioadmin123
```

#### 5. Python Environment Issues
```bash
# Recreate virtual environment
deactivate
rm -rf venv
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Environment-Specific Notes

#### Windows
- Use PowerShell or Git Bash for commands
- Ensure Docker Desktop is running
- Set environment variables in System Properties
- Use `venv\Scripts\activate` for virtual environment

#### macOS
- Install Homebrew first: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
- Use `brew install` for dependencies
- Ensure Xcode Command Line Tools are installed

#### Linux
- Install Docker and Docker Compose
- Ensure user is in docker group: `sudo usermod -aG docker $USER`
- May need to install additional packages: `sudo apt-get update && sudo apt-get install -y openjdk-11-jdk`

## Next Steps

After completing this setup, you'll be ready to:

1. **Run the complete data pipeline**: Bronze → Silver → Gold
2. **Ingest Vietnamese market data**: From VNDirect, SSI, and TCBS APIs
3. **Process real-time streams**: Through Kafka and Spark Streaming
4. **Store in Delta Lake**: With ACID transactions and time travel
5. **Build trading strategies**: Using the analytics-ready gold layer
6. **Monitor the system**: Through Grafana dashboards

### Verification Checklist

- [ ] Python virtual environment activated
- [ ] All services running in Docker
- [ ] Kafka topics created and accessible
- [ ] Spark and Delta Lake working
- [ ] Database schema initialized
- [ ] MinIO buckets created
- [ ] Configuration files updated with your API keys
- [ ] Integration tests passing
- [ ] Demo scripts running successfully

**🎉 Your Vietnamese Algorithmic Trading System data infrastructure is now ready for production!**