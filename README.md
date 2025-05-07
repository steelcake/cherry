# cherry 

[![PyPI](https://img.shields.io/badge/PyPI-lightgreen?style=for-the-badge&logo=pypi&labelColor=white)](https://pypi.org/project/cherry-etl/)
[![Telegram](https://img.shields.io/badge/Telegram-darkgray?style=for-the-badge&logo=telegram)](https://t.me/cherry_etl)
[![Documentation](https://img.shields.io/badge/documentation-blue?style=for-the-badge&logo=readthedocs)](https://steelcake.github.io/cherry-docs/)
[![GitHub](https://img.shields.io/badge/github-black?style=for-the-badge&logo=github)](https://github.com/steelcake/cherry)

Cherry is a python library for building blockchain data pipelines.

It is designed to make building production-ready blockchain data pipelines easy.

## Getting Started

See [getting started section](https://steelcake.github.io/cherry-docs/getting_started.html) of the docs.

## Features

- Pure `python` library. Don't need yaml, SQL, toml etc. 
- High-level `datasets` API and flexible pipeline API.
- `High-performance`, `low-cost` and `uniform` data access. Ability to use advanced providers without platform lock-in.
- Included functionality to `decode`, `validate`, `transform` blockchain data. All implemented in `rust` for performance. 
- Write transformations using `polars`, `pyarrow`, `datafusion`, `pandas`, `duckdb` or any other `pyarrow` compatible library.
- `Schema inference` automatically creates output tables. 
- Keep datasets fresh with `continuous ingestion`.
- `Parallelized`, next batch of data is being fetched while your pre-processing function is running, while the database writes are being executed in parallel. Don't need to hand optimize anything.
- Included library of transformations.
- Included functionality to implement `crash-resistance`.

## Data providers

| Provider            | Ethereum (EVM) | Solana (SVM)  |
|---------------------|----------------|---------------|
| [HyperSync](https://docs.envio.dev/docs/HyperSync/overview) | ✅ | ❌ |
| [SQD](https://docs.sqd.ai/)             | ✅ | ✅ |
| [Yellowstone-GRPC](https://github.com/rpcpool/yellowstone-grpc) | ❌ | ✅ |

## Supported output formats

- **ClickHouse**
- **Iceberg**
- **Deltalake**
- **DuckDB**
- **Arrow Datasets**
- **Parquet**

## Usage examples

- [**Ethereum(EVM)** examples](examples/using_datasets/eth)
- [**Solana(SVM)** examples](examples/using_datasets/svm)

## Logging

Python code uses the standard `logging` module of python, so it can be configured according to [python docs](https://docs.python.org/3/library/logging.html).

Set `RUST_LOG` environment variable according to [env_logger docs](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) in order to see logs from rust modules.

To run an example with trace level logging for rust modules:
```
RUST_LOG=trace uv run examples/path/to/my/example
```

## Development

This repo uses `uv` for development.

- Format the code with `uv run ruff format`
- Lint the code with `uv run ruff check`
- Run type checks with `uv run pyright`
- Run the tests with `uv run pytest`

Core libraries we use for ingesting/decoding/validating/transforming blockchain data are implemented in [cherry-core](https://github.com/steelcake/cherry-core) repo.

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

## Sponsors

[<img src="https://steelcake.com/envio-logo.png" width="150px" />](https://envio.dev)
[<img src="https://steelcake.com/sqd-logo.png" width="165px" />](https://sqd.ai)
[<img src="https://steelcake.com/space-operator-logo.webp" height="75px" />](https://linktr.ee/spaceoperator)


