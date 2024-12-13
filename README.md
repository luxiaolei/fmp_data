# FMP Data Client

A robust Python client for Financial Modeling Prep (FMP) API with data persistence, caching, and automated data collection capabilities.

## Features

### Core Features (v1.0)
- Async API client for efficient data fetching
- Daily historical price data for stocks, indices, forex, and crypto
- Data persistence in MongoDB with automatic schema validation
- Redis-based caching and rate limiting
- Automated data health checks and gap detection
- Timezone-aware data handling
- Comprehensive logging with loguru
- CLI for data downloads and updates
- Background tasks for data ingestion
- Configurable cron jobs for routine updates

### Planned Features (v2.0)
- 5-minute intraday data support
- Real-time market data updates
- Support for fundamental data (financial statements, ratios, etc.)
- News and sentiment data integration
- Advanced caching strategies
- Data export capabilities (CSV, Parquet)
- REST API service layer

## Installation

```bash
pip install fmp-data
```

## Quick Start

```python
from fmp_data import FMPDataClient

# Initialize client
client = FMPDataClient()

# Get daily historical data
df = await client.get_daily_data(
    symbol="AAPL",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Get current S&P 500 constituents
sp500 = await client.get_index_constituents("sp500")

# Get historical constituent changes
changes = await client.get_historical_constituents("sp500")
```

## Configuration

The client can be configured via environment variables or a config file:

```yaml
# config.yaml
api:
  key: YOUR_FMP_API_KEY
  base_url: https://financialmodelingprep.com/api/v3
  rate_limit: 740  # requests per minute

storage:
  mongodb:
    uri: mongodb://localhost:27017
    db_name: fmp_data
  redis:
    url: redis://localhost:6379
    password: YOUR_REDIS_PASSWORD

logging:
  level: INFO
  format: "{time} | {level} | {message}"
  rotation: "1 day"
```

## Project Structure

```
fmp_data/
├── src/
│   ├── api/              # API client modules
│   ├── models/           # Data models and schemas
│   ├── storage/          # Database and cache handlers
│   ├── collectors/       # Data collection jobs
│   ├── utils/           # Helper functions
│   └── config/          # Configuration handling
├── tests/               # Test suite
└── examples/            # Usage examples
```

## Environment Variables

Required environment variables:
- `FMP_API_KEY`: Your FMP API key
- `MONGO_URI`: MongoDB connection URI
- `MONGO_DB_NAME`: MongoDB database name
- `REDIS_URL`: Redis connection URL
- `REDIS_PASSWORD`: Redis password

## Contributing

Contributions are welcome! Please check our contribution guidelines.

## License

MIT License

## CLI Usage

```bash
# Download historical data for a symbol
fmp download --symbol AAPL --start-date 2024-01-01

# Download all symbols
fmp download

# Update index constituents
fmp update-indexes

# Start cron job for routine updates
fmp start-cron
```
