# FMP Data Client

A robust Python client for Financial Modeling Prep (FMP) API with data persistence and automated data collection capabilities.

## Features

### Current Features
- Async API client for efficient data fetching
- Historical price data support:
  - Daily data with automatic gap detection
  - 5-minute intraday data
- MongoDB-based data persistence with:
  - Automatic schema validation
  - Data integrity checks
  - Gap detection and tracking
- Index constituent tracking:
  - S&P 500
  - NASDAQ
  - Dow Jones
- Comprehensive data statistics and monitoring
- Rich CLI interface with progress tracking
- Timezone-aware data handling
- Configurable logging with loguru

## TODOs

- [ ] Add index history data
- [ ] Add constituent history data
- [ ] Hanlde routine updates for prices data, index constituents, and company profiles
  
## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/fmp-data.git
cd fmp-data

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root:

```env
FMP_API_KEY=your_api_key
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=fmp_data
```

## CLI Usage

```bash
# Start interactive mode
python -m fmp_data.cli.main

# Available commands:
python -m fmp_data.cli.main interactive  # Interactive mode
python -m fmp_data.cli.main update-indexes  # Update index constituents
python -m fmp_data.cli.main test  # Run system tests
python -m fmp_data.cli.main routine-update  # Run routine data updates
```

### Interactive Mode Features
1. Download historical data (single symbol or all symbols)
2. Update index constituents
3. View data statistics
4. Run routine updates
5. Set log level
6. Database management

## Project Structure

```
fmp_data/
├── cli/
│   └── main.py          # CLI implementation
├── src/
│   ├── api/             # API client implementation
│   │   └── fmp_client.py
│   ├── config/          # Configuration handling
│   │   └── settings.py
│   ├── models/          # Data models
│   │   └── data_types.py
│   ├── storage/         # Data persistence
│   │   ├── mongo.py
│   │   ├── protocols.py
│   │   └── schemas.py
│   └── utils/           # Utilities
│       ├── calendar.py
│       ├── date_utils.py
│       └── tasks.py
```

## Data Statistics

View comprehensive statistics about your data:
- Total symbols tracked
- Daily and 5-minute data points
- Date range coverage
- Symbol distribution by exchange
- Symbol distribution by asset type

## Error Handling

The client includes:
- Comprehensive error logging
- Automatic retry mechanisms
- Data validation
- Gap detection and reporting

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

MIT License

## Acknowledgments

- Financial Modeling Prep API
- MongoDB
- Rich CLI library
