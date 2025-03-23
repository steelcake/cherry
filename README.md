# Cherry  [<img src="https://steelcake.com/telegram-logo.png" width="30px" />](https://t.me/cherryframework) 

[![PyPI version](https://badge.fury.io/py/cherry-etl.svg)](https://pypi.org/project/cherry-etl/)

Cherry is a python library for building blockchain data pipelines.

It is designed to make building production-ready blockchain data pipelines easy.

## Features

- Don't need SQL or external config files. Only write `python`. Able to create `self-contained` python scripts and utilize the dynamic nature of python.
- `High-performance` and `low-cost` proprietary data sources are available `without the downside of platform lock-in`. Just change two lines to switch between data providers.
- Prebuilt functionality to `decode`, `validate`, `transform` blockchain data. All implemented in `rust` for performance. Including `UInt256`, `ethereum hex`, `solana base58` encoding/decoding functionality, and more.
- Support for both `Ethereum (EVM)` and `Solana (SVM)` based blockchains. More to come.
- Write data into `Clickhouse`, `Iceberg`, `Deltalake`, `DuckDB`, `Parquet` and any other supported platform. Can switch between writers without changing any other part of the pipeline.
- `Schema inference`, don't need to manually create and manage database schemas, cherry figures out how it should create the tables so you don't have to.
- Keep datasets fresh with `continuous ingestion`.
- `Fully parallelized and optimized architecture`. Next batch of data is being fetched while your pre-processing function is running, while the database writes are being executed in parallel. Don't need to hand optimize anything.
- Write transformations in any `Arrow` compatible library, `polars`, `pandas`, `datafusion`, `duckdb` and so on.
- Prebuilt library of transformations e.g. encode all binary columns to `ethereum prefixed-hex` format or `solana base58` format strings.
- Prebuilt functionality to implement `crash-resistance`. Make your pipeline crash resistant so it doesn't lose data and starts from where it left off in case of a crash.

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

- [**Ethereum(EVM)** examples](examples/eth)
- [**Solana(SVM)** examples](examples/solana)

## Logging

Python code uses the standard `logging` module of python, so it can be configured according to [python docs](https://docs.python.org/3/library/logging.html).

Set `RUST_LOG` environment variable according to [env_logger docs](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) in order to see logs from rust modules.

To run an example with trace level logging for rust modules:
```
RUST_LOG=trace uv run examples/{example_name}/main.py --provider {sqd or hypersync}
```

## Development

This repo uses `uv` for development.

- Format the code with `uv run ruff format`
- Lint the code with `uv run ruff check`
- Run type checks with `uv run pyright`

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

