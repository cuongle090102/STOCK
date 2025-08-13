#!/usr/bin/env python3
"""
MinIO Setup Script for Vietnamese Algorithmic Trading System
Creates necessary buckets for the data lake architecture.
"""

import sys
import time
from pathlib import Path

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("❌ MinIO client not installed. Run: pip install minio")
    sys.exit(1)


def setup_minio_buckets():
    """Set up MinIO buckets for the data lake."""
    print("🗄️  Setting up MinIO buckets for Vietnamese Market Data Lake...")
    
    # Initialize MinIO client
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
    
    # Test connection
    try:
        client.list_buckets()
        print("✅ Successfully connected to MinIO")
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        print("💡 Make sure MinIO is running: docker-compose up -d")
        return False
    
    # Create buckets for data lake layers
    buckets = [
        {
            "name": "bronze-market-data",
            "description": "Raw market data from Vietnamese exchanges"
        },
        {
            "name": "silver-market-data", 
            "description": "Validated and cleansed market data"
        },
        {
            "name": "gold-analytics-data",
            "description": "Analytics-ready data with technical indicators"
        },
        {
            "name": "trading-signals",
            "description": "Generated trading signals from strategies"
        },
        {
            "name": "risk-events",
            "description": "Risk management events and alerts"
        },
        {
            "name": "backups",
            "description": "Database and configuration backups"
        },
        {
            "name": "exports",
            "description": "Data exports and reports"
        },
        {
            "name": "model-artifacts",
            "description": "Machine learning models and artifacts"
        }
    ]
    
    created_count = 0
    for bucket in buckets:
        try:
            bucket_name = bucket["name"]
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"✅ Created bucket: {bucket_name} - {bucket['description']}")
                created_count += 1
            else:
                print(f"ℹ️  Bucket already exists: {bucket_name}")
        except S3Error as e:
            print(f"❌ Error creating bucket {bucket_name}: {e}")
            return False
    
    print(f"\n🎉 MinIO setup complete! Created {created_count} new buckets")
    print(f"🌐 MinIO Console: http://localhost:9001")
    print(f"🔑 Login: minioadmin / minioadmin123")
    
    return True


def verify_minio_setup():
    """Verify MinIO setup by listing buckets and testing upload."""
    print("\n🔍 Verifying MinIO setup...")
    
    try:
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        
        # List all buckets
        buckets = client.list_buckets()
        print(f"📦 Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"   - {bucket.name} (created: {bucket.creation_date})")
        
        # Test upload to bronze bucket
        test_content = "Test file for Vietnamese market data system"
        test_file_path = Path("test_upload.txt")
        test_file_path.write_text(test_content)
        
        client.fput_object(
            "bronze-market-data",
            "test/test_upload.txt",
            str(test_file_path)
        )
        
        # Test download
        client.fget_object(
            "bronze-market-data", 
            "test/test_upload.txt",
            "downloaded_test.txt"
        )
        
        # Clean up test files
        test_file_path.unlink()
        Path("downloaded_test.txt").unlink()
        
        print("✅ Upload/download test successful")
        print("✅ MinIO is ready for production use")
        
        return True
        
    except Exception as e:
        print(f"❌ MinIO verification failed: {e}")
        return False


def main():
    """Main setup function."""
    print("Vietnamese Algorithmic Trading System - MinIO Setup")
    print("=" * 60)
    
    # Wait a moment for MinIO to be ready if just started
    print("⏳ Waiting for MinIO to be ready...")
    time.sleep(5)
    
    # Setup buckets
    if not setup_minio_buckets():
        print("❌ MinIO setup failed")
        return False
    
    # Verify setup
    if not verify_minio_setup():
        print("❌ MinIO verification failed")
        return False
    
    print("\n" + "=" * 60)
    print("✅ MinIO Setup Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Access MinIO Console at http://localhost:9001")
    print("2. Run: python validate_config.py")
    print("3. Start data ingestion pipeline")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)