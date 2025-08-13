# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Vietnamese Algorithmic Trading System - a production-ready algorithmic trading platform with real-time analytics, backtesting capabilities, and institutional-grade risk management. The system supports Vietnamese equities markets with comprehensive data ingestion, strategy development, and live trading capabilities.

**Tech Stack**: Python, PySpark, Kafka, Delta Lake, Apache Superset, Apache Airflow, Docker, PostgreSQL, Redis, MinIO, Grafana, Prometheus

## Common Commands

### Development Setup
```bash
make setup-all          # Complete project setup (recommended for first-time)
make install-dev         # Install development dependencies
make setup-dev          # Setup development environment
```

### Code Quality & Testing
```bash
make test               # Run all tests with coverage
make test-unit          # Run unit tests only
make test-integration   # Run integration tests only
make test-performance   # Run performance tests only
make lint               # Run linting checks (ruff, black, isort)
make type-check         # Run mypy type checking
make format             # Format code (black, ruff, isort)
make pre-commit         # Run all pre-commit hooks
```

### Docker Services
```bash
make docker-up          # Start all services (Kafka, Superset, Grafana, etc.)
make docker-down        # Stop all services
make docker-build       # Build Docker images
make docker-logs        # Show logs from all services
```

### Demo & Development
```bash
make demo-trading       # Start paper trading demo
make demo-backtest      # Run backtesting demo
make demo-analytics     # Launch complete analytics suite
make open-superset      # Open Superset dashboard (http://localhost:8088)
make open-grafana       # Open Grafana dashboard (http://localhost:3000)
make open-jupyter       # Open Jupyter Lab (http://localhost:8888)
```

### Data Management
```bash
make migration          # Run database migrations
make seed-data          # Load sample data for development
make backup             # Backup database and data
make restore            # Restore from backup
```

## Architecture Overview

### Core Components

1. **Data Ingestion Layer** (`src/ingestion/`)
   - Multi-source data extraction with fallback mechanisms
   - Real-time and historical data support
   - Data validation and quality checks
   - Base classes: `DataSource`, `DataExtractor`, `MarketDataPoint`

2. **Analytics & Visualization**
   - **Apache Superset**: Primary BI platform for executive dashboards, risk analytics, strategy performance
   - **Grafana**: System monitoring and operational metrics
   - **Jupyter Lab**: Interactive analysis and development notebooks

3. **Strategy Framework** (`src/strategies/`)
   - Abstract base strategy classes
   - Signal generation and position management
   - Backtesting and walk-forward analysis capabilities

4. **Real-time Processing**
   - **Apache Kafka**: Event streaming for market data
   - **PySpark Structured Streaming**: Real-time data processing
   - **Delta Lake**: ACID-compliant data lakehouse

5. **Trading Engine** (`src/live/`)
   - Paper trading simulation
   - Risk management and position tracking
   - Order management system

6. **Infrastructure Services**
   - **PostgreSQL**: Application metadata, Airflow, Superset
   - **Redis**: Caching and Celery task queue
   - **MinIO**: S3-compatible object storage
   - **Apache Airflow**: Workflow orchestration

### Project Structure
```
src/
├── ingestion/          # Data ingestion components
├── strategies/         # Trading strategy implementations
├── backtest/          # Backtesting engine
├── live/              # Live/paper trading
├── indicators/        # Technical indicators library
└── utils/             # Shared utilities and logging

docker/                # Container configurations
├── docker-compose.yml # All services definition
├── trading/           # Trading app container
└── postgres/          # Database initialization

configs/               # Configuration files
data/                  # Data storage (bronze/silver/gold)
dags/                  # Airflow DAGs
notebooks/             # Jupyter analysis notebooks
tests/                 # Comprehensive test suite
```

## Development Guidelines

### Testing Strategy
- Unit tests in `tests/unit/` with `pytest`
- Integration tests in `tests/integration/`
- Performance tests in `tests/performance/`
- Target: >85% code coverage
- Use markers: `-m "unit"`, `-m "integration"`, `-m "performance"`

### Code Quality Tools
- **Black**: Code formatting (line length: 100)
- **Ruff**: Fast Python linter with comprehensive rules
- **MyPy**: Static type checking (strict mode enabled)
- **Pre-commit**: Automated quality checks

### Configuration Management
- Uses `pyproject.toml` for all Python tooling configuration
- Environment variables via `.env` file (use `.env.template`)
- Structured logging with Loguru (`src/utils/logging.py`)

### Docker Development
- Development environment: `make docker-up`
- Services accessible at:
  - Superset: http://localhost:8088 (admin/admin)
  - Grafana: http://localhost:3000 (admin/admin123)
  - Jupyter: http://localhost:8888 (token: vnalgo123)
  - Airflow: http://localhost:8080
  - Trading API: http://localhost:8000

### Performance Considerations
- C++ indicators library planned for performance-critical calculations
- PySpark for distributed data processing
- Kafka for high-throughput streaming
- Delta Lake for efficient data queries with time travel

### Logging & Monitoring
- Structured JSON logging via Loguru
- Specialized loggers: `StructuredLogger` class
- Trading events, market data, risk alerts, and performance metrics
- Prometheus metrics integration
- Grafana dashboards for system monitoring

## Key Features

- **Multi-source data ingestion** with automatic failover
- **Real-time Vietnamese market data** processing
- **Advanced backtesting** with transaction cost modeling
- **Risk management** with VaR, drawdown analysis, and position limits
- **Apache Superset dashboards** for executive reporting and analytics
- **Paper trading** simulation with realistic execution
- **Comprehensive monitoring** and alerting
- **Production-ready infrastructure** with Docker orchestration

## Business Intelligence Integration

The system heavily integrates with **Apache Superset** for analytics:
- Executive overview dashboards with P&L and risk metrics
- Strategy performance comparison and attribution analysis
- Risk management console with VaR heatmaps and stress testing
- Market microstructure analysis and execution quality monitoring

Run `make demo-analytics` to launch the complete analytics suite.