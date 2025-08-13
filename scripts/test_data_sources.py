#!/usr/bin/env python3
"""Test script for Vietnamese market data sources."""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.ingestion.vietnamese_extractor import VietnameseMarketExtractor
from src.ingestion.vnd_direct import VNDDirectClient
from src.ingestion.ssi_client import SSIFastConnectClient
from src.ingestion.tcbs_client import TCBSClient
from src.utils.logging import setup_logging, StructuredLogger


def test_individual_sources():
    """Test each data source individually."""
    logger = StructuredLogger("DataSourceTest")
    
    # Test symbols
    test_symbols = ["VIC", "VNM", "HPG"]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    logger.info("Starting individual data source tests")
    
    # Test VND Direct
    logger.info("Testing VND Direct...")
    try:
        vnd_config = {
            "base_url": "https://api.vndirect.com.vn",
            "timeout": 30,
            "rate_limit": 60
        }
        
        vnd_client = VNDDirectClient(vnd_config)
        
        if vnd_client.connect():
            logger.info("VND Direct connection successful")
            
            # Test health check
            health = vnd_client.health_check()
            logger.info(f"VND Direct health: {health}")
            
            # Test historical data
            for symbol in test_symbols:
                data = vnd_client.get_historical_data(symbol, start_date, end_date)
                logger.info(f"VND Direct {symbol}: {len(data)} records")
                
                if not data.empty:
                    logger.info(f"Sample data for {symbol}:")
                    logger.info(f"  Date range: {data['timestamp'].min()} to {data['timestamp'].max()}")
                    logger.info(f"  Price range: {data['close_price'].min():.2f} - {data['close_price'].max():.2f}")
                    logger.info(f"  Avg volume: {data['volume'].mean():.0f}")
        else:
            logger.error("VND Direct connection failed")
            
        vnd_client.disconnect()
        
    except Exception as e:
        logger.error(f"VND Direct test failed: {e}")
    
    # Test TCBS
    logger.info("Testing TCBS...")
    try:
        tcbs_config = {
            "base_url": "https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance",
            "timeout": 30,
            "rate_limit": 100
        }
        
        tcbs_client = TCBSClient(tcbs_config)
        
        if tcbs_client.connect():
            logger.info("TCBS connection successful")
            
            # Test health check
            health = tcbs_client.health_check()
            logger.info(f"TCBS health: {health}")
            
            # Test historical data
            for symbol in test_symbols:
                data = tcbs_client.get_historical_data(symbol, start_date, end_date)
                logger.info(f"TCBS {symbol}: {len(data)} records")
                
                if not data.empty:
                    logger.info(f"Sample data for {symbol}:")
                    logger.info(f"  Date range: {data['timestamp'].min()} to {data['timestamp'].max()}")
                    logger.info(f"  Price range: {data['close_price'].min():.2f} - {data['close_price'].max():.2f}")
                    logger.info(f"  Avg volume: {data['volume'].mean():.0f}")
            
            # Test additional features
            company_info = tcbs_client.get_company_info("VIC")
            if company_info:
                logger.info(f"VIC company info: {company_info}")
            
            market_indices = tcbs_client.get_market_indices()
            if market_indices:
                logger.info(f"Market indices: {market_indices}")
                
        else:
            logger.error("TCBS connection failed")
            
        tcbs_client.disconnect()
        
    except Exception as e:
        logger.error(f"TCBS test failed: {e}")
    
    # Test SSI (Note: requires credentials)
    logger.info("Testing SSI FastConnect...")
    try:
        ssi_config = {
            "base_url": "https://iboard.ssi.com.vn",
            "ws_url": "wss://iboard.ssi.com.vn/ws",
            "consumer_id": os.getenv("SSI_CONSUMER_ID"),
            "consumer_secret": os.getenv("SSI_CONSUMER_SECRET"),
            "timeout": 30
        }
        
        ssi_client = SSIFastConnectClient(ssi_config)
        
        if ssi_client.config_model.consumer_id and ssi_client.config_model.consumer_secret:
            if ssi_client.connect():
                logger.info("SSI connection successful")
                
                # Test health check
                health = ssi_client.health_check()
                logger.info(f"SSI health: {health}")
                
                # Test historical data
                for symbol in test_symbols:
                    data = ssi_client.get_historical_data(symbol, start_date, end_date)
                    logger.info(f"SSI {symbol}: {len(data)} records")
            else:
                logger.error("SSI connection failed")
        else:
            logger.warning("SSI credentials not available - skipping SSI test")
            logger.info("Set SSI_CONSUMER_ID and SSI_CONSUMER_SECRET environment variables to test SSI")
            
        ssi_client.disconnect()
        
    except Exception as e:
        logger.error(f"SSI test failed: {e}")


def test_vietnamese_extractor():
    """Test the Vietnamese market extractor."""
    logger = StructuredLogger("ExtractorTest")
    
    logger.info("Testing VietnameseMarketExtractor")
    
    try:
        # Create extractor
        extractor = VietnameseMarketExtractor()
        
        # Connect to sources
        connection_results = extractor.connect_all_sources()
        logger.info(f"Connection results: {connection_results}")
        
        # Health check
        health_status = extractor.get_comprehensive_health_check()
        logger.info(f"Health status: {health_status}")
        
        # Test data fetching with fallback
        test_symbol = "VIC"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        logger.info(f"Testing data fetch for {test_symbol}")
        data = extractor.get_historical_data(test_symbol, start_date, end_date)
        
        if not data.empty:
            logger.info(f"Successfully fetched {len(data)} records for {test_symbol}")
            
            # Validate data quality
            quality_report = extractor.validate_data_quality(data, test_symbol)
            logger.info(f"Data quality report: {quality_report}")
        else:
            logger.warning(f"No data available for {test_symbol}")
        
        # Test multi-source comparison
        logger.info("Testing multi-source data comparison")
        multi_source_data = extractor.get_multi_source_data(test_symbol, start_date, end_date)
        
        for source_name, source_data in multi_source_data.items():
            logger.info(f"{source_name}: {len(source_data)} records")
        
        # Test market overview
        market_overview = extractor.get_market_overview()
        logger.info(f"Market overview: {market_overview}")
        
    except Exception as e:
        logger.error(f"Vietnamese extractor test failed: {e}")


def test_sample_vn30_data():
    """Test fetching sample VN30 data."""
    logger = StructuredLogger("VN30Test")
    
    logger.info("Testing VN30 sample data fetch")
    
    try:
        extractor = VietnameseMarketExtractor()
        extractor.connect_all_sources()
        
        # Fetch data for top 5 VN30 stocks
        top5_symbols = ["VIC", "VNM", "HPG", "VCB", "FPT"]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=14)  # 2 weeks
        
        results = {}
        
        for symbol in top5_symbols:
            logger.info(f"Fetching data for {symbol}")
            data = extractor.get_historical_data(symbol, start_date, end_date)
            
            if not data.empty:
                results[symbol] = data
                logger.info(f"{symbol}: {len(data)} records, latest price: {data['close_price'].iloc[-1]:.2f}")
            else:
                logger.warning(f"No data for {symbol}")
        
        # Save sample data
        data_dir = Path(__file__).parent.parent / "data" / "samples"
        data_dir.mkdir(parents=True, exist_ok=True)
        
        for symbol, data in results.items():
            filename = data_dir / f"{symbol}_sample_{datetime.now().strftime('%Y%m%d')}.csv"
            data.to_csv(filename, index=False)
            logger.info(f"Saved sample data: {filename}")
        
        logger.info(f"Successfully fetched data for {len(results)}/{len(top5_symbols)} symbols")
        
    except Exception as e:
        logger.error(f"VN30 sample test failed: {e}")


def main():
    """Main test function."""
    # Setup logging
    setup_logging(log_level="INFO", log_format="text")
    
    logger = StructuredLogger("TestMain")
    logger.info("Starting Vietnamese market data sources test")
    
    print("=" * 60)
    print("Vietnamese Market Data Sources Test")
    print("=" * 60)
    
    try:
        # Test individual sources
        print("\n1. Testing individual data sources...")
        test_individual_sources()
        
        print("\n2. Testing Vietnamese market extractor...")
        test_vietnamese_extractor()
        
        print("\n3. Testing VN30 sample data fetch...")
        test_sample_vn30_data()
        
        print("\n" + "=" * 60)
        print("Test completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"\nTest failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()