# TASK4CLAUDE.md

# 🚀 Vietnamese Algorithmic Trading System - Claude Implementation Tasks

This document provides Claude Code with specific implementation tasks and development guidance for the Vietnamese Algorithmic Trading System project.

## 🎯 Current Project Status & Phase

**Project**: Production-ready algorithmic trading system for Vietnamese equities market  
**Current Phase**: Implementation and Development  
**Tech Stack**: Python 3.11+, PySpark, Kafka, Delta Lake, Apache Superset, PostgreSQL, Docker  
**Architecture**: Multi-layered system with real-time streaming, analytics, and trading capabilities  

## 🛠️ Available Development Environment

### Key Services (via `make docker-up`)
- **Apache Superset**: http://localhost:8088 (admin/admin) - Primary BI platform
- **Grafana**: http://localhost:3000 (admin/admin123) - System monitoring  
- **Jupyter Lab**: http://localhost:8888 (token: vnalgo123) - Analysis environment
- **Airflow**: http://localhost:8080 - Workflow orchestration
- **PostgreSQL**: localhost:5432 - Primary database (vnalgo/vnalgo123)
- **Kafka**: localhost:9092 - Event streaming
- **MinIO**: http://localhost:9000 - Object storage
- **Trading API**: http://localhost:8000 - Main application

### Essential Commands
```bash
make setup-all          # Complete project setup
make test               # Run comprehensive test suite  
make lint               # Code quality checks (ruff, black, mypy)
make docker-up          # Start all infrastructure services
make demo-trading       # Launch paper trading demonstration
make demo-analytics     # Start analytics dashboards
```

## 📋 High-Priority Implementation Tasks

### Phase 1: Data Infrastructure & Ingestion

#### 1.1 Vietnamese Market Data Sources Implementation
**Priority: HIGH**
- [ ] **VND Direct API Integration** (`src/ingestion/vnd_direct.py`)
  - Implement authentication and rate limiting
  - Create OHLCV data extraction for VN30 stocks
  - Add error handling and circuit breaker patterns
  - Target symbols: VIC, VNM, HPG, VCB, FPT, GAS, BID, CTG, MSN, PLX

- [ ] **SSI FastConnect API Client** (`src/ingestion/ssi_client.py`)
  - Build WebSocket connection for real-time data
  - Implement heartbeat monitoring and reconnection logic
  - Create message parsing for tick data
  - Add data normalization to standard format

- [ ] **TCBS API Wrapper** (`src/ingestion/tcbs_client.py`)
  - Historical data fetching with date range support
  - Corporate actions and dividend data integration
  - Market indices (VN-Index, VN30) data extraction
  - Data quality validation and cleansing

- [ ] **Multi-Source Data Extractor** (`src/ingestion/extractor.py`)
  - Extend existing `DataExtractor` class with Vietnamese sources
  - Implement source prioritization and failover logic
  - Add data reconciliation between sources
  - Create comprehensive health check system

#### 1.2 Apache Kafka Streaming Pipeline
**Priority: HIGH**
- [ ] **Kafka Producer for Market Data** (`src/streaming/producers.py`)
  - Real-time tick data publishing to Kafka topics
  - Schema Registry integration for message versioning
  - Batch publishing for historical data backfill
  - Add producer metrics and monitoring

- [ ] **Kafka Consumer Groups** (`src/streaming/consumers.py`)
  - Strategy signal consumers with offset management
  - Risk management event consumers
  - Analytics data consumers for Superset integration
  - Dead letter queue handling for failed messages

- [ ] **PySpark Streaming Jobs** (`jobs/streaming/`)
  - Tick-to-bar aggregation with watermarking
  - Real-time technical indicator calculation
  - Market data validation and quality checks
  - Stream-to-Delta Lake persistence

#### 1.3 Delta Lake Data Architecture
**Priority: MEDIUM**
- [ ] **Bronze Layer (Raw Data)** (`src/datalake/bronze/`)
  - Raw market data ingestion with schema enforcement
  - Data partitioning by date and symbol
  - ACID transaction support for data updates
  - Time travel capabilities for data recovery

- [ ] **Silver Layer (Validated Data)** (`src/datalake/silver/`)
  - Data quality validation and cleansing
  - Standardized schema across all data sources
  - Deduplication and data reconciliation
  - Historical data reconstruction and gap filling

- [ ] **Gold Layer (Analytics-Ready)** (`src/datalake/gold/`)
  - Pre-calculated technical indicators
  - Strategy-specific feature engineering
  - Aggregated market statistics and benchmarks
  - Performance attribution data for reporting

### Phase 2: Trading Strategy Framework

#### 2.1 Strategy Engine Development
**Priority: HIGH**
- [ ] **Base Strategy Classes** (`src/strategies/base.py`)
  - Abstract strategy interface with signal generation
  - Parameter validation and configuration management
  - State persistence for strategy recovery
  - Performance tracking and attribution

- [ ] **Mean Reversion Strategy** (`src/strategies/mean_reversion.py`)
  - Z-score based entry/exit signals
  - Dynamic threshold adjustment based on volatility
  - Sector-neutral implementation for Vietnamese market
  - Risk-adjusted position sizing

- [ ] **Momentum Strategy** (`src/strategies/momentum.py`)
  - Multi-timeframe momentum detection
  - Trend strength filtering and confirmation
  - Volume-weighted momentum indicators
  - Regime-aware parameter adjustment

- [ ] **Pairs Trading Strategy** (`src/strategies/pairs_trading.py`)
  - Cointegration testing for Vietnamese stock pairs
  - Statistical arbitrage signal generation
  - Market-neutral portfolio construction
  - Alpha decay monitoring and strategy refresh

#### 2.2 Real-Time Signal Generation
**Priority: HIGH**
- [ ] **Signal Engine** (`src/live/signals.py`)
  - Real-time strategy execution with Kafka integration
  - Multi-strategy signal aggregation and ranking
  - Signal filtering based on market conditions
  - Signal persistence and audit trail

- [ ] **Technical Indicators Library** (`src/indicators/`)
  - Vectorized implementations using NumPy/Pandas
  - Support for streaming calculations
  - Vietnamese market-specific indicators
  - Performance optimization with Cython/C++

### Phase 3: Risk Management & Analytics

#### 3.1 Real-Time Risk Management
**Priority: HIGH**
- [ ] **Position Manager** (`src/risk/positions.py`)
  - Real-time portfolio tracking and exposure monitoring
  - Dynamic position sizing based on volatility
  - Sector and single-name concentration limits
  - Foreign ownership compliance for Vietnamese market

- [ ] **Risk Calculator** (`src/risk/calculator.py`)
  - Value-at-Risk (VaR) calculations using historical simulation
  - Expected Shortfall and tail risk metrics
  - Correlation-based portfolio risk decomposition
  - Stress testing with Vietnamese market scenarios

- [ ] **Risk Alerts System** (`src/risk/alerts.py`)
  - Real-time risk limit monitoring
  - Automated position adjustment triggers
  - Emergency stop-loss mechanisms
  - Risk reporting dashboard integration

#### 3.2 Apache Superset Analytics Integration
**Priority: MEDIUM**
- [ ] **Superset Dashboards** (`docs/superset/`)
  - Executive portfolio overview with P&L attribution
  - Strategy performance comparison and benchmarking
  - Risk management console with VaR heatmaps
  - Market microstructure analysis dashboards

- [ ] **Data Connection Setup** (`configs/superset/`)
  - PostgreSQL and Delta Lake connections
  - Security configuration and access controls
  - Dashboard export/import automation
  - Custom visualization plugins

### Phase 4: Paper Trading & Backtesting

#### 4.1 Paper Trading Engine
**Priority: HIGH**
- [ ] **Paper Trader** (`src/live/paper_trader.py`)
  - Realistic order execution simulation
  - Slippage and market impact modeling
  - Portfolio P&L calculation and tracking
  - Trade history persistence and analysis

- [ ] **Order Management System** (`src/live/orders.py`)
  - Order validation and risk checks
  - Partial fill simulation based on volume
  - Order status tracking and updates
  - Cancel/replace order handling

#### 4.2 Backtesting Framework
**Priority: MEDIUM**
- [ ] **Backtest Engine** (`src/backtest/engine.py`)
  - Walk-forward analysis with out-of-sample testing
  - Transaction cost modeling (0.15% brokerage + market impact)
  - Multiple strategy backtesting and comparison
  - Monte Carlo simulation for robustness testing

- [ ] **Performance Analytics** (`src/backtest/analytics.py`)
  - Risk-adjusted return metrics (Sharpe, Sortino, Calmar)
  - Drawdown analysis and recovery time calculation
  - Trade-level analysis with win/loss statistics
  - Strategy performance attribution and factor analysis

### Phase 5: Monitoring & Operations

#### 5.1 System Monitoring
**Priority: MEDIUM**
- [ ] **Health Checks** (`src/monitoring/health.py`)
  - Service health monitoring with automatic recovery
  - Data quality monitoring and alerting
  - Latency monitoring for real-time components
  - Database performance and connection monitoring

- [ ] **Grafana Dashboards** (`docker/grafana/dashboards/`)
  - System performance metrics and alerts
  - Trading system operational metrics
  - Infrastructure monitoring (CPU, memory, network)
  - Business metrics and KPI tracking

#### 5.2 Operational Tools
**Priority: LOW**
- [ ] **CLI Tools** (`src/cli/`)
  - System administration and maintenance commands
  - Data export and import utilities
  - Strategy deployment and configuration tools
  - Operational runbook automation

## 🎯 Implementation Guidelines for Claude

### Code Quality Standards
- Follow existing patterns in `src/ingestion/base.py` and `src/utils/logging.py`
- Use structured logging with `StructuredLogger` class
- Implement comprehensive error handling and circuit breakers
- Add type hints and docstrings following Google style
- Write unit tests with >85% coverage target

### Vietnamese Market Considerations
- Handle VND currency with proper precision (no decimal places)
- Respect Vietnamese market hours (9:00-11:30, 13:00-15:00 local time)
- Implement foreign ownership monitoring and compliance
- Use Vietnamese stock symbols (VIC, VNM, HPG, etc.)
- Handle market holidays and trading suspensions

### Performance Requirements
- Sub-2 second latency for signal generation
- Process >1000 market data points per second
- 99.9% uptime during trading hours
- <1% missing or invalid data points
- Handle historical data processing for multi-year datasets

### Integration Patterns
- Use existing Docker services (PostgreSQL, Kafka, MinIO)
- Integrate with Apache Superset for analytics dashboards
- Follow Airflow DAG patterns for workflow orchestration
- Use Delta Lake for all analytical data storage
- Implement Kafka patterns for real-time communication

### Testing Strategy
- Unit tests in `tests/unit/` with pytest
- Integration tests with Docker services
- Performance tests for latency-critical components
- Use markers for test categorization (`-m "unit"`)
- Mock external API calls in tests

## 🚀 Quick Start for Implementation

1. **Environment Setup**:
   ```bash
   make setup-all
   make docker-up
   ```

2. **Start with Data Ingestion**:
   - Implement Vietnamese data source in `src/ingestion/`
   - Test with `make test-integration`
   - Validate data quality with sample datasets

3. **Add Streaming Components**:
   - Create Kafka producers/consumers
   - Implement PySpark streaming jobs
   - Monitor with Grafana dashboards

4. **Build Trading Strategies**:
   - Extend base strategy framework
   - Implement backtesting validation
   - Add paper trading simulation

5. **Configure Analytics**:
   - Set up Superset dashboards
   - Create performance monitoring
   - Add risk management alerts

## 📊 Success Metrics

- **Technical**: <2s latency, >1000 msg/sec throughput, 99.9% uptime
- **Trading**: >1.2 Sharpe ratio, <10% max drawdown, >60% monthly wins
- **Code Quality**: >85% test coverage, zero critical security issues
- **Vietnamese Market**: Full VN30 coverage with regulatory compliance

This document provides Claude with the context and specific tasks needed to contribute effectively to the Vietnamese Algorithmic Trading System project.