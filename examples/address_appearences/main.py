from clickhouse_connect.driver.asyncclient import AsyncClient
import pyarrow as pa
from cherry import config as cc
from cherry.config import (
    ClickHouseSkipIndex,
    StepKind,
    HexEncodeConfig,
)
from cherry import run_pipelines, Context
from cherry_core import ingest
import logging
import os
import asyncio
import clickhouse_connect
from dotenv import load_dotenv
import traceback
from typing import Dict, Optional
import argparse
import time

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)


FALLBACK_BLOCK = 17422044


async def get_start_block(client: AsyncClient) -> int:
    try:
        res = await client.query("SELECT MAX(block_number) FROM address_appearances")
        return res.result_rows[0][0] or FALLBACK_BLOCK
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return FALLBACK_BLOCK


async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    logger.info(f"joining data: {data.keys()}")

    transactions = data["transactions"]

    logger.info(f"joined data: {transactions.num_rows}")

    # Combine the chunks into single arrays first
    from_addr = transactions.column("from").combine_chunks()
    to_addr = transactions.column("to").combine_chunks()
    block_number = transactions.column("block_number").combine_chunks()

    # Create arrays with duplicated block numbers to match the from/to addresses
    block_numbers_expanded = pa.concat_arrays(
        [
            block_number,  # One copy for 'from' addresses
            block_number,  # Another copy for 'to' addresses
        ]
    )

    # Combine all addresses
    addresses = pa.concat_arrays([from_addr, to_addr])

    # Create table with both columns
    out = pa.Table.from_arrays(
        [addresses, block_numbers_expanded],
        names=["address", "block_number"],
    )

    return {"address_appearances": out}


async def main(provider_kind: ingest.ProviderKind, url: Optional[str]):
    clickhouse_client = await clickhouse_connect.get_async_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "blockchain"),
    )

    from_block = await get_start_block(clickhouse_client)
    logger.info(f"starting to ingest from block {from_block}")

    provider = cc.Provider(
        name="address_tracker",
        config=ingest.ProviderConfig(
            kind=provider_kind,
            url=url,
            query=ingest.Query(
                kind=ingest.QueryKind.EVM,
                params=ingest.evm.Query(
                    from_block=from_block,
                    to_block=from_block + 1000,
                    include_all_blocks=True,
                    transactions=[ingest.evm.TransactionRequest()],
                    fields=ingest.evm.Fields(
                        block=ingest.evm.BlockFields(number=True, timestamp=True),
                        transaction=ingest.evm.TransactionFields(
                            block_number=True,
                            from_=True,
                            to=True,
                        ),
                    ),
                ),
            ),
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=clickhouse_client,
            order_by={"address_appearances": ["block_number"]},
            codec={"address_appearances": {"data": "ZSTD"}},
            skip_index={
                "address_appearances": [
                    ClickHouseSkipIndex(
                        name="address_idx",
                        val="address",
                        type_="bloom_filter(0.01)",
                        granularity=1,
                    ),
                ]
            },
        ),
    )

    config = cc.Config(
        project_name="address_appearances",
        description="Track address appearances in transactions",
        pipelines={
            "address_pipeline": cc.Pipeline(
                provider=provider,
                writer=writer,
                steps=[
                    cc.Step(
                        name="join_data",
                        kind="join_data",
                    ),
                    cc.Step(
                        name="prefix_hex_encode",
                        kind=StepKind.HEX_ENCODE,
                        config=HexEncodeConfig(),
                    ),
                ],
            )
        },
    )

    context = Context()
    context.add_step("join_data", join_data)
    await run_pipelines(config, context)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Address appearances tracker")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

    url = None
    if args.provider == ingest.ProviderKind.HYPERSYNC:
        url = "https://eth.hypersync.xyz"
    elif args.provider == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/ethereum-mainnet"

    start_time = time.time()
    asyncio.run(main(args.provider, url))
    end_time = time.time()
    logger.info(f"time taken: {end_time - start_time} seconds")
