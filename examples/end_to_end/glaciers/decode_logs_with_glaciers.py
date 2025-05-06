# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/end_to_end/glaciers/decode_logs_with_glaciers.py --provider hypersync --from_block 20000000 --to_block 20000001

# After run, you can see the result in the database:
# duckdb data/glaciers_decoded_logs.db
# SELECT * FROM decoded_logs LIMIT 3;
# SELECT * FROM ethereum_uni_v2_trades LIMIT 3;

import argparse
import asyncio
import logging
import os
import requests
import pyarrow as pa

from typing import Optional
from pathlib import Path

import duckdb
from cherry_core import ingest

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.glaciers")

# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


PROVIDER_URLS = {
    ingest.ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ingest.ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}


async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    abi_db_path: str,
    from_block: int,
    provider_url: Optional[str],
    to_block: Optional[int],
):
    # Ensure to_block is not None, use from_block + 10 as default if it is
    actual_to_block = to_block if to_block is not None else from_block + 10

    logger.info(
        f"starting to ingest from block {from_block} to block {actual_to_block}"
    )

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=provider_url,
    )

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=actual_to_block,
            include_all_blocks=True,
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    hash=True,
                    number=True,
                    timestamp=True,
                ),
                log=ingest.evm.LogFields(
                    address=True,
                    data=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    block_number=True,
                    block_hash=True,
                    transaction_hash=True,
                    transaction_index=True,
                    log_index=True,
                    removed=True,
                ),
                transaction=ingest.evm.TransactionFields(
                    from_=True,
                    to=True,
                    value=True,
                    status=True,
                    block_hash=True,
                    block_number=True,
                    hash=True,
                ),
            ),
            logs=[ingest.evm.LogRequest()],
            transactions=[ingest.evm.TransactionRequest()],
        ),
    )

    # Create the pipeline using the logs dataset
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                ),
            ),
            cc.Step(
                kind=cc.StepKind.GLACIERS_EVENTS,
                config=cc.GlaciersEventsConfig(
                    abi_db_path=abi_db_path,
                ),
            ),
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="glaciers", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    from_block: int,
    provider_url: Optional[str],
    to_block: Optional[int],
):
    url = "https://github.com/yulesa/glaciers/raw/refs/heads/master/ABIs/ethereum__events__abis.parquet"
    abi_db_path = "data/ethereum__events__abis.parquet"

    if not os.path.exists(abi_db_path):
        response = requests.get(url)
        with open(abi_db_path, "wb") as file:
            file.write(response.content)

    connection = duckdb.connect("data/glaciers_decoded_logs.db")

    # sync the data into duckdb
    await sync_data(
        connection=connection.cursor(),
        provider_kind=provider_kind,
        abi_db_path=abi_db_path,
        from_block=from_block,
        provider_url=provider_url,
        to_block=to_block,
    )

    # Optional: read result to show
    data = connection.sql(
        "SELECT name, FIRST(event_values), FIRST(event_keys), FIRST(event_json), FIRST(transaction_hash), COUNT(*) AS evt_count FROM decoded_logs GROUP BY name ORDER BY evt_count DESC"
    )
    logger.info(f"\n{data}")

    # DB Operations - Create tables
    connection.sql(
        "CREATE OR REPLACE TABLE eth_tokens AS SELECT * FROM read_csv('examples/end_to_end/glaciers/eth_tokens.csv');"
    )
    connection.sql(
        "CREATE OR REPLACE TABLE ethereum_uni_v2_pools AS SELECT * FROM read_csv('examples/end_to_end/glaciers/ethereum_uni_v2_pools.csv');"
    )

    # Create uniswap v2 trade table using the decoded_logs dataset
    connection.sql(
        """
        CREATE OR REPLACE TABLE ethereum_uni_v2_trades AS
        WITH exploded_swaps AS (
            SELECT 
                timestamp,
                address as pool_address,
                from_json(json(event_values), '["VARCHAR"]') as evt_values,
                evt_values[1] AS swap_sender,
                evt_values[2] AS swap_to,
                TRY_CAST(evt_values[3] AS HUGEINT) AS amount0in,
                TRY_CAST(evt_values[4] AS HUGEINT) AS amount1in,
                TRY_CAST(evt_values[5] AS HUGEINT) AS amount0out,
                TRY_CAST(evt_values[6] AS HUGEINT) AS amount1out,
                transaction_hash,
                block_number,
                log_index,
                transaction_index
            FROM decoded_logs
            WHERE name = 'Swap'
            AND address IN (SELECT pair from ethereum_uni_v2_pools)
        )

        SELECT 
            'uniswap_v2' as protocol,
            swaps.timestamp,
            swaps.pool_address,
            token0.symbol AS token_sold_symbol,
            token1.symbol AS token_bought_symbol,
            token0.symbol || '-' || token1.symbol AS token_pair,
            swaps.amount0in AS token_sold_amount_raw,
            swaps.amount1out AS token_bought_amount_raw,
            swaps.amount0in/10^token0.decimals AS token_sold_amount,
            swaps.amount1out/10^token1.decimals AS token_bought_amount,
            token0.contract_address AS token_sold_address,
            token1.contract_address AS token_bought_address,
            swaps.swap_sender,
            swaps.swap_to,
            swaps.transaction_hash,
            swaps.block_number,
            swaps.log_index,
            swaps.transaction_index
        FROM exploded_swaps AS swaps
        LEFT JOIN ethereum_uni_v2_pools AS pools ON swaps.pool_address = pools.pair
        LEFT JOIN eth_tokens AS token0 ON pools.token0 = token0.contract_address
        LEFT JOIN eth_tokens AS token1 ON pools.token1 = token1.contract_address
        WHERE swaps.amount0in >= swaps.amount1in

        UNION ALL

        SELECT 
            'uniswap_v2' as protocol,
            swaps.timestamp,
            swaps.pool_address,
            token1.symbol AS token_sold_symbol,
            token0.symbol AS token_bought_symbol,
            token0.symbol || '-' || token1.symbol AS token_pair,
            swaps.amount1in AS token_sold_amount_raw,
            swaps.amount0out AS token_bought_amount_raw,
            swaps.amount1in/10^token1.decimals AS token_sold_amount,
            swaps.amount0out/10^token0.decimals AS token_bought_amount,
            token1.contract_address AS token_sold_address,
            token0.contract_address AS token_bought_address,
            swaps.swap_sender,
            swaps.swap_to,
            swaps.transaction_hash,
            swaps.block_number,
            swaps.log_index,
            swaps.transaction_index
        FROM exploded_swaps AS swaps
        LEFT JOIN ethereum_uni_v2_pools AS pools ON swaps.pool_address = pools.pair
        LEFT JOIN eth_tokens AS token0 ON pools.token0 = token0.contract_address
        LEFT JOIN eth_tokens AS token1 ON pools.token1 = token1.contract_address
        WHERE swaps.amount0in < swaps.amount1in;
        """
    )
    # Optional: read result to show
    data = connection.sql("SELECT * FROM ethereum_uni_v2_trades LIMIT 3;")
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Blocks tracker")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )
    parser.add_argument(
        "--from_block",
        required=True,
        help="Specify the block to start from",
    )
    parser.add_argument(
        "--to_block",
        required=False,
        help="Specify the block to stop at, inclusive",
    )

    args = parser.parse_args()

    url = PROVIDER_URLS[args.provider]

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, from_block, url, to_block))
