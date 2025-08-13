#!/usr/bin/env python3
"""
Configuration Validation Script for Vietnamese Algorithmic Trading System
Validates all configuration settings and service connections.
"""

import os
import sys
import time
import socket
from pathlib import Path
from dotenv import load_dotenv


def check_service_port(host: str, port: int, service_name: str) -> bool:
    """Check if a service is running on specified host:port."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"[OK] {service_name} is running on {host}:{port}")
            return True
        else:
            print(f"[ERROR] {service_name} is not accessible on {host}:{port}")
            return False
    except Exception as e:
        print(f"[ERROR] Error checking {service_name}: {e}")
        return False


def validate_environment_variables():
    """Validate required environment variables."""
    print("[INFO] Validating environment variables...")
    
    # Load .env file if it exists
    env_file = Path(".env")
    if env_file.exists():
        load_dotenv()
        print(f"[OK] Loaded environment from {env_file}")
    else:
        print(f"[WARN] No .env file found. Using system environment variables.")
        print(f"[INFO] Copy .env.example to .env and configure your settings")
    
    # Check required variables
    required_vars = [
        'DATABASE_URL',
        'KAFKA_BOOTSTRAP_SERVERS',
        'SPARK_MASTER',
        'MINIO_ENDPOINT',
    ]
    
    # Check optional but recommended variables
    recommended_vars = [
        'VNDIRECT_API_KEY',
        'SSI_CONSUMER_ID',
        'TCBS_API_URL',
        'LOG_LEVEL',
        'ENVIRONMENT'
    ]
    
    missing_required = []
    missing_recommended = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)
        else:
            print(f"[OK] {var}: {os.getenv(var)}")
    
    for var in recommended_vars:
        if not os.getenv(var):
            missing_recommended.append(var)
        else:
            # Mask sensitive values
            value = os.getenv(var)
            if 'KEY' in var or 'SECRET' in var or 'PASSWORD' in var:
                value = '*' * len(value) if value else 'Not set'
            print(f"[OK] {var}: {value}")
    
    if missing_required:
        print(f"[ERROR] Missing required environment variables: {missing_required}")
        return False
    
    if missing_recommended:
        print(f"[WARN]  Missing recommended environment variables: {missing_recommended}")
        print(f"💡 These are needed for full functionality")
    
    return True


def validate_directory_structure():
    """Validate and create required directory structure."""
    print("\n[INFO] Validating directory structure...")
    
    required_dirs = [
        'data/delta/bronze',
        'data/delta/silver',
        'data/delta/gold',
        'data/samples',
        'data/exports',
        'data/backups',
        'data/demo_results',
        'configs',
        'logs',
        'notebooks',
        'sql',
        'spark-warehouse',
        'checkpoints'
    ]
    
    for dir_path in required_dirs:
        path = Path(dir_path)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"[OK] Created directory: {dir_path}")
        else:
            print(f"[OK] Directory exists: {dir_path}")
    
    return True


def validate_services():
    """Validate that required services are running."""
    print("\n🔌 Validating service connections...")
    
    services = [
        ("localhost", 5432, "PostgreSQL"),
        ("localhost", 9092, "Kafka"),
        ("localhost", 8081, "Schema Registry"),
        ("localhost", 9000, "MinIO"),
        ("localhost", 9001, "MinIO Console"),
    ]
    
    optional_services = [
        ("localhost", 3000, "Grafana"),
        ("localhost", 8888, "Jupyter"),
        ("localhost", 8080, "Kafka UI"),
    ]
    
    service_status = {}
    
    # Check required services
    for host, port, name in services:
        status = check_service_port(host, port, name)
        service_status[name] = status
    
    # Check optional services
    print(f"\nOptional services:")
    for host, port, name in optional_services:
        check_service_port(host, port, name)
    
    # Check if all required services are running
    required_services = [name for _, _, name in services]
    failed_services = [name for name in required_services if not service_status.get(name, False)]
    
    if failed_services:
        print(f"\n[ERROR] Failed services: {failed_services}")
        print(f"💡 Start services with: docker-compose up -d")
        return False
    
    print(f"\n[OK] All required services are running")
    return True


def test_database_connection():
    """Test database connection."""
    print("\n🗄️  Testing database connection...")
    
    try:
        import psycopg2
        from urllib.parse import urlparse
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            print("[ERROR] DATABASE_URL not configured")
            return False
        
        # Parse database URL
        parsed = urlparse(database_url)
        
        # Test connection
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port,
            database=parsed.path[1:],  # Remove leading slash
            user=parsed.username,
            password=parsed.password
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"[OK] Database connection successful")
        print(f"[OK] PostgreSQL version: {version}")
        
        # Check if our schemas exist
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('market_data', 'strategies', 'risk_management', 'monitoring');")
        schemas = cursor.fetchall()
        
        if len(schemas) >= 4:
            print(f"[OK] Database schemas initialized: {[s[0] for s in schemas]}")
        else:
            print(f"[WARN]  Some database schemas missing. Run: docker-compose up -d to initialize")
        
        cursor.close()
        conn.close()
        return True
        
    except ImportError:
        print("[ERROR] psycopg2 not installed. Run: pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return False


def test_kafka_connection():
    """Test Kafka connection."""
    print("\n📨 Testing Kafka connection...")
    
    try:
        from kafka import KafkaAdminClient, KafkaProducer
        from kafka.admin import NewTopic
        
        # Test admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='validation_client'
        )
        
        # List topics
        metadata = admin_client.list_consumer_groups()
        print("[OK] Kafka admin client connected")
        
        # Test producer
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.close()
        print("[OK] Kafka producer test successful")
        
        return True
        
    except ImportError:
        print("[ERROR] kafka-python not installed. Run: pip install kafka-python")
        return False
    except Exception as e:
        print(f"[ERROR] Kafka connection failed: {e}")
        print("💡 Make sure Kafka is running: docker-compose -f docker-compose.kafka.yml up -d")
        return False


def test_minio_connection():
    """Test MinIO connection."""
    print("\n🗃️  Testing MinIO connection...")
    
    try:
        from minio import Minio
        
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        
        # List buckets
        buckets = client.list_buckets()
        print(f"[OK] MinIO connection successful")
        print(f"[OK] Found {len(buckets)} buckets: {[b.name for b in buckets]}")
        
        return True
        
    except ImportError:
        print("[ERROR] minio not installed. Run: pip install minio")
        return False
    except Exception as e:
        print(f"[ERROR] MinIO connection failed: {e}")
        print("💡 Make sure MinIO is running: docker-compose up -d")
        return False


def validate_python_packages():
    """Validate required Python packages are installed."""
    print("\n[INFO] Validating Python packages...")
    
    required_packages = [
        'pandas',
        'numpy',
        'pyspark',
        'kafka-python',
        'psycopg2',
        'minio',
        'requests',
        'pydantic',
        'loguru'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"[OK] {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"[ERROR] {package}")
    
    if missing_packages:
        print(f"\n[ERROR] Missing packages: {missing_packages}")
        print(f"[INFO] Install with: pip install {' '.join(missing_packages)}")
        return False
    
    return True


def main():
    """Main validation function."""
    print("Vietnamese Algorithmic Trading System - Configuration Validation")
    print("=" * 70)
    
    validation_results = {}
    
    # Run all validations
    validation_results['environment'] = validate_environment_variables()
    validation_results['directories'] = validate_directory_structure()
    validation_results['packages'] = validate_python_packages()
    validation_results['services'] = validate_services()
    
    # Test service connections
    if validation_results['services']:
        validation_results['database'] = test_database_connection()
        validation_results['kafka'] = test_kafka_connection()
        validation_results['minio'] = test_minio_connection()
    else:
        print("[WARN]  Skipping service connection tests (services not running)")
        validation_results['database'] = False
        validation_results['kafka'] = False
        validation_results['minio'] = False
    
    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    
    total_checks = len(validation_results)
    passed_checks = sum(validation_results.values())
    
    for check, result in validation_results.items():
        status = "[OK] PASS" if result else "[ERROR] FAIL"
        print(f"{check.upper().replace('_', ' '):20} {status}")
    
    print(f"\nOverall: {passed_checks}/{total_checks} checks passed")
    
    if passed_checks == total_checks:
        print("\n🎉 All validations passed! System is ready for use.")
        print("\nNext steps:")
        print("1. Run: python setup_kafka_topics.py")
        print("2. Run: python scripts/demo_delta_lake_final.py")
        print("3. Access services:")
        print("   - Kafka UI: http://localhost:8080")
        print("   - MinIO Console: http://localhost:9001")
        print("   - Grafana: http://localhost:3000")
        return True
    else:
        print(f"\n[ERROR] {total_checks - passed_checks} validation(s) failed. Please fix the issues above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)