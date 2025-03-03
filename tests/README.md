# Cherry Tests

This directory contains tests for the Cherry ETL library.

## Running Tests

To run the tests, use the following command from the project root:

```bash
uv run pytest
```

For more verbose output:

```bash
uv run pytest -v
```

To run a specific test file:

```bash
uv run pytest tests/writers/test_delta_lake_writer.py -v
```

## Required Dependencies

Make sure to install the required test dependencies:

```bash
uv add pytest pytest-asyncio
``` 