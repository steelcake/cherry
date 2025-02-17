# Cherry Event Indexer

A flexible blockchain event indexing and data processing pipeline.

## Overview

Cherry Event Indexer is a modular system for:
- Ingesting blockchain events and logs
- Processing and transforming blockchain data
- Writing data to various storage backends

## Features

- **Modular Pipeline Architecture**
  - Configurable data providers
  - Customizable processing steps
  - Pluggable storage backends

- **Built-in Steps**
  - EVM block validation
  - Event decoding
  - Custom processing steps

- **Storage Options**
  - Local Parquet files
  - AWS S3
  - More coming soon...

## Project Structure:

```
cherry/
├── src/
│ ├── config/ # Configuration parsing
│ ├── utils/ # Pipeline and utilities
│ └── writers/ # Storage backends
├── examples/ # Example implementations
├── tests/ # Test suite
└── config.yaml # Pipeline configuration
```

## Prerequisites:
- Python 3.10 or higher
- Docker and Docker Compose
- MinIO (for local S3-compatible storage)

## Installation Steps

Clone the repository and go to the project root:

```bash
git clone https://github.com/steelcake/cherry.git
cd cherry
```

Create and activate a virtual environment:

```bash
# Create virtual environment (all platforms)
python -m venv .venv

# Activate virtual environment

# For Windows with git bash:
source .venv/Scripts/activate

# For macOS/Linux:
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Set up environment variables:

Create a .env file in the project root
Add your Hypersync API token:

## Quick Start

1. Create a config file (`config.yaml`):

2. Run the script:

```bash
python main.py
```

## Custom Processing Steps

To add a custom processing step, you need to:

1. Define the step function
2. Add the step to the context
3. Add the step to the config

example: get_block_number_stats.py
```python
def get_block_number_stats(data: Dict[str, pa.RecordBatch], step_config: Dict[str, Any]) -> Dict[str, pa.RecordBatch]:
    """Custom processing step for transfer events"""
    pass
```

config.yaml
```yaml
steps:
  - name: my_get_block_number_stats
    kind: get_block_number_stats
    config:
      input_table: logs
      output_table: block_number_stats
```

## Running the Project

Start MinIO server (for local S3 storage):

```bash
# Navigate to docker-compose directory
cd docker-compose

# Start MinIO using docker-compose
docker-compose up -d

# Return to project root
cd ..
```

Default credentials:

```
Access Key: minioadmin
Secret Key: minioadmin
Console URL: http://localhost:9001
```

Note: The MinIO service will be automatically configured with the correct ports and volumes as defined in the docker-compose.yml file.

Configure pipelines:

- Open config.yaml
- Adjust query, event filters, and batch sizes as needed for your pipeline
- Configure writer settings (S3/local parquet etc.)

Run the indexer:

```bash
python main.py
```

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.