<img src="https://steelcake.com/cherry-brand-logo.jpg" width="350px" />

# Cherry 
[![PyPI version](https://badge.fury.io/py/cherry-etl.svg)](https://badge.fury.io/py/cherry-etl)

Python framework for building blockchain data pipelines.  

<br/>

Cherry is in the early stages of development, so the API is changing, and we are still figuring things out.

We would love to help you get started and get your feedback on [our telegram channel](https://t.me/cherryframework).

Core libraries we use for ingesting/decoding/validating/transforming blockchain data are implemented in [cherry-core](https://github.com/steelcake/cherry-core) repo.

<b> This project is sponsored by: </b>

[<img src="https://steelcake.com/envio-logo.png" width="150px" />](https://envio.dev)
[<img src="https://steelcake.com/sqd-logo.png" width="165px" />](https://sqd.ai)
[<img src="https://steelcake.com/space-operator-logo.webp" height="75px" />](https://linktr.ee/spaceoperator)

## Features

- Ingest data from multiple providers with a uniform interface. This makes switching providers as easy as changing a couple lines in config.
- Prebuilt functionality to decode/validate/transform blockchain data.
- Support for both Ethereum (EVM) and Solana (SVM) based blockchains.
- Write data into Clickhouse, Iceberg, Deltalake, Parquet/Arrow (via pyarrow).
- Keep datasets fresh with continuous ingestion.

## Status

We are still trying to figure out our core use cases and trying to build up to them. Here is a rough roadmap::

- Add option to ingest Solana data from geyser plugin/RPC.
- Add option to ingest EVM data from Ethereum RPC.
- Implement more advanced validation.
- Add more writers like DuckDB, PostgreSQL.
- Build an end-to-end testing flow so we can test the framework and users can test their pipelines using the same flow.
- Build a benchmark flow so we can optimize the framework and user can optimize their pipelines using the same flow. This will also make it easy to compare performance of providers and writers. 
- Implement more blockchain formats like SUI, Aptos, Fuel.
- Implement automatic rollback handling. Currently we don't handle rollbacks so we stay behind the tip of the chain in order to avoid writing wrong data.

## Usage Examples

See `examples` directory.

Can run examples with `uv run examples/{example_name}/main.py --provider {sqd or hypersync}`

For examples that require databases or other infra, run `docker-compose -f examples/{example_name}/docker-compose.yaml up -d` to start the necessary docker containers.
Can run `docker-compose -f examples/{example_name}/docker-compose.yaml down -v` after running the example to stop the docker containers and delete the data.

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
