# Cherry Blockchain Indexer

A high-performance blockchain event indexing and data processing pipeline that uses Hypersync to efficiently process and store Ethereum event data.

## Project Structure
```
cherry/
├── src/                      # Source code
│   ├── config/              # Configuration parsing
│   │   ├── __init__.py
│   │   └── parser.py       # Config file parser
│   ├── ingesters/          # Data ingestion
│   │   ├── __init__.py
│   │   ├── base.py        # Base ingester class
│   │   ├── factory.py     # Ingester factory
│   │   └── providers/     # Data source providers
│   │       └── hypersync.py # Hypersync ingester
│   ├── processors/         # Data processing
│   │   ├── __init__.py
│   │   └── hypersync.py   # Hypersync data processor
│   ├── schemas/           # Data schemas
│   │   ├── __init__.py
│   │   ├── base.py       # Schema converter
│   │   └── blockchain_schemas.py
│   ├── types/            # Type definitions
│   │   ├── __init__.py
│   │   ├── data.py      # Data container
│   │   └── hypersync.py # Hypersync types
│   ├── utils/           # Utilities
│   │   ├── __init__.py
│   │   ├── logging_setup.py
│   │   ├── schema_converter.py
│   │   └── generate_hypersync_query.py
│   └── writers/         # Data writers
│       ├── __init__.py
│       ├── base.py     # Base writer class
│       ├── parquet.py  # Parquet writer
│       ├── postgres.py # PostgreSQL writer
│       ├── s3.py      # S3/MinIO writer
│       └── writer.py  # Writer manager
├── data/              # Output data directory
├── logs/             # Application logs
├── state/            # Stream state files
├── docker-compose/   # Docker configurations
├── config.yaml       # Main configuration
├── main.py          # Application entry point
├── requirements.txt  # Python dependencies
└── README.md        # Documentation
```

## Prerequisites

- Python 3.10 or higher
- Docker and Docker Compose
- MinIO (for local S3-compatible storage)

## Installation Steps

1. Clone the repository and go to the project root:

```bash
git clone https://github.com/steelcake/cherry.git
cd cherry
```

2. Create and activate a virtual environment:

```bash
# Create virtual environment (all platforms)
python -m venv .venv

# Activate virtual environment

# For Windows with git bash:
source .venv/Scripts/activate

# For macOS/Linux:
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up environment variables:
   - Create a `.env` file in the project root
   - Add your Hypersync API token:

```
HYPERSYNC_API_TOKEN=your_token_here
```

## Running the Project

1. Start MinIO server (for local S3 storage):

```bash
# Navigate to docker-compose directory
cd docker-compose

# Start MinIO using docker-compose
docker-compose up -d

# Return to project root
cd ..
```

Default credentials:
- Access Key: minioadmin
- Secret Key: minioadmin
- Console URL: http://localhost:9001

Note: The MinIO service will be automatically configured with the correct ports and volumes as defined in the docker-compose.yml file.

2. Configure event streams:
   - Open `config.yaml`
   - Adjust block ranges, event filters, and batch sizes as needed
   - Configure output settings (S3/local parquet)

3. Run the indexer:
```bash
python main.py
```

## Data Output Locations

### Local Parquet Files
```
data/
├── events/
│   ├── approval/
│   │   ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   │   ├── ...
│   │   └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   └── transfer/
│       ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│       ├── ...
│       └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
├── blocks/
│   ├── approval/
│   │   ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   │   ├── ...
│   │   └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   └── transfer/
│       ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│       ├── ...
│       └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
```

### S3/MinIO Storage
```
blockchain-data/
├── events/
│   ├── approval/
│   │   ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   │   ├── ...
│   │   └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   └── transfer/
│       ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│       ├── ...
│       └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
├── blocks/
│   ├── approval/
│   │   ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   │   ├── ...
│   │   └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│   └── transfer/
│       ├── YYYYMMDD_HHMMSS_startblock_endblock.parquet
│       ├── ...
│       └── YYYYMMDD_HHMMSS_startblock_endblock.parquet
```


Access via:
- MinIO Console: http://localhost:9001
- S3 Endpoint: http://localhost:9000

## Monitoring

1. Check application logs:
   - Located in `logs/` directory
   - Format: `blockchain_etl_YYYYMMDD_HHMMSS.log`

2. Monitor processing progress:
   - Console output shows real-time processing stats
   - Log files contain detailed processing information

3. View processed data:
   - Local: Check `data/` directory
   - S3: Access MinIO console at http://localhost:9001
   - Data is organized by event type and timestamp
   - Each file contains events from a specific block range

## State Management

The project maintains processing state in the `state/` directory:
```
state/
├── approval_stream.json
├── transfer_stream.json
└── block_stream.json
```

These files track the last processed block for each stream and enable resume functionality.

## Troubleshooting

1. If no data is being processed:
   - Verify your Hypersync API token
   - Check block range configuration
   - Ensure event filters are correctly set

2. If MinIO connection fails:
   - Verify MinIO is running (`docker ps`)
   - Check credentials in config.yaml
   - Ensure ports 9000 and 9001 are available

3. For other issues:
   - Check the latest log file in `logs/` directory
   - Verify configuration in config.yaml
   - Ensure all requirements are installed correctly
   - Check Docker logs: `docker-compose logs minio`