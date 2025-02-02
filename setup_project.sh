#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}Creating Data Collection Project Structure...${NC}"

# Create root directory
mkdir -p data_collection

# Create all directories
directories=(
    "src/core"
    "src/providers/alpha_vantage"
    "src/providers/yahoo_finance"
    "src/models"
    "src/utils"
    "src/services"
    "src/validation"
    "tests/unit/test_providers"
    "tests/unit/test_services"
    "tests/unit/test_validation"
    "tests/integration/test_data_collection"
    "config"
    "logs"
    "cache"
)

# Create directories
for dir in "${directories[@]}"; do
    mkdir -p "data_collection/$dir"
    echo "Created directory: $dir"
done

# Create __init__.py files in all Python directories
find data_collection/src -type d -exec touch {}/__init__.py \;
find data_collection/tests -type d -exec touch {}/__init__.py \;

# Create core files
core_files=(
    "src/core/base_classes.py"
    "src/core/exceptions.py"
    "src/core/interfaces.py"
    "src/core/constants.py"
)

# Create provider files
provider_files=(
    "src/providers/provider_factory.py"
    "src/providers/alpha_vantage/client.py"
    "src/providers/alpha_vantage/parser.py"
    "src/providers/alpha_vantage/models.py"
    "src/providers/yahoo_finance/client.py"
    "src/providers/yahoo_finance/parser.py"
    "src/providers/yahoo_finance/models.py"
)

# Create model files
model_files=(
    "src/models/data_models.py"
    "src/models/schema.py"
    "src/models/validators.py"
)

# Create utility files
util_files=(
    "src/utils/rate_limiter.py"
    "src/utils/logger.py"
    "src/utils/cache_manager.py"
    "src/utils/error_handler.py"
)

# Create service files
service_files=(
    "src/services/data_collector.py"
    "src/services/data_processor.py"
    "src/services/data_validator.py"
    "src/services/data_store.py"
)

# Create validation files
validation_files=(
    "src/validation/validation_rules.py"
    "src/validation/data_validators.py"
    "src/validation/data_sanitizers.py"
)

# Create config files
config_files=(
    "config/settings.py"
    "config/providers.yaml"
    "config/logging.yaml"
)

# Create root level files
root_files=(
    "requirements.txt"
    "setup.py"
    "README.md"
)

# Combine all files
all_files=(
    "${core_files[@]}"
    "${provider_files[@]}"
    "${model_files[@]}"
    "${util_files[@]}"
    "${service_files[@]}"
    "${validation_files[@]}"
    "${config_files[@]}"
    "${root_files[@]}"
)

# Create all files
for file in "${all_files[@]}"; do
    touch "data_collection/$file"
    echo "Created file: $file"
done

# Create .gitkeep in empty directories
touch "data_collection/cache/.gitkeep"
touch "data_collection/logs/.gitkeep"

# Create basic README content
cat > data_collection/README.md << EOL
# Data Collection System

## Overview
This system handles data collection from multiple financial data sources with rate limiting and validation.

## Project Structure
- src/: Source code
- tests/: Unit and integration tests
- config/: Configuration files
- logs/: Application logs
- cache/: Temporary data storage

## Setup
1. Create virtual environment: python -m venv venv
2. Activate virtual environment: source venv/bin/activate
3. Install dependencies: pip install -r requirements.txt

## Usage
[Add usage instructions here]

## Development
[Add development instructions here]
EOL

# Create basic requirements.txt
cat > data_collection/requirements.txt << EOL
# API Clients
requests==2.31.0
yfinance==0.2.33

# Data Processing
pandas==2.1.3
numpy==1.26.2

# Validation
pydantic==2.5.2

# Configuration
PyYAML==6.0.1

# Async Support
aiohttp==3.9.1
asyncio==3.4.3

# Testing
pytest==7.4.3
pytest-asyncio==0.21.1

# Logging
loguru==0.7.2

# Cache
cachetools==5.3.2
EOL

# Create basic setup.py
cat > data_collection/setup.py << EOL
from setuptools import setup, find_packages

setup(
    name="data_collection",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'requests',
        'yfinance',
        'pandas',
        'numpy',
        'pydantic',
        'PyYAML',
        'aiohttp',
        'asyncio',
        'loguru',
        'cachetools',
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A system for collecting financial data from multiple sources",
    keywords="finance, data collection, api",
    url="http://github.com/yourusername/data_collection",
)
EOL

# Make the cache directory writable
chmod 777 data_collection/cache

# Create basic logging config
cat > data_collection/config/logging.yaml << EOL
version: 1
formatters:
  simple:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: simple
    stream: ext://sys.stdout
  file:
    class: logging.FileHandler
    level: INFO
    formatter: simple
    filename: logs/app.log
root:
  level: INFO
  handlers: [console, file]
EOL

# Create basic providers config
cat > data_collection/config/providers.yaml << EOL
alpha_vantage:
  base_url: "https://www.alphavantage.co/query"
  rate_limit:
    calls_per_minute: 5
    calls_per_day: 500
  endpoints:
    intraday: "TIME_SERIES_INTRADAY"
    daily: "TIME_SERIES_DAILY"
    quote: "GLOBAL_QUOTE"

yahoo_finance:
  rate_limit:
    calls_per_second: 2
    calls_per_hour: 2000
  features:
    - historical_data
    - real_time_quotes
    - company_info
EOL

echo -e "${GREEN}Project structure created successfully!${NC}"
echo -e "${GREEN}Next steps:${NC}"
echo "1. cd data_collection"
echo "2. python -m venv venv"
echo "3. source venv/bin/activate"
echo "4. pip install -r requirements.txt"