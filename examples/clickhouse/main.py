from clickhouse_connect.driver.asyncclient import AsyncClient
import pyarrow as pa
from cherry import config as cc
from cherry.config import (
    ClickHouseSkipIndex,
    StepKind,
    EvmDecodeEventsConfig,
    HexEncodeConfig,
    CastConfig,
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

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)


async def get_start_block(client: AsyncClient) -> int:
    try:
        res = await client.query("SELECT MAX(block_number) FROM transfers")
        return res.result_rows[0][0] or 0
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return 0


async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    blocks = blocks.rename_columns(["block_number", "block_timestamp"])
    out = transfers.join(blocks, keys="block_number")

    return {"transfers": out}


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
        name="my_provider",
        config=ingest.ProviderConfig(
            kind=provider_kind,
            url=url,
            query=ingest.Query(
                kind=ingest.QueryKind.EVM,
                params=ingest.evm.Query(
                    from_block=from_block,
                    logs=[
                        ingest.evm.LogRequest(
                            address=["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
                            event_signatures=["Transfer(address,address,uint256)"],
                        )
                    ],
                    fields=ingest.evm.Fields(
                        block=ingest.evm.BlockFields(number=True, timestamp=True),
                        log=ingest.evm.LogFields(
                            block_number=True,
                            log_index=True,
                            address=True,
                            topic0=True,
                            topic1=True,
                            topic2=True,
                            topic3=True,
                            data=True,
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
            order_by={"transfers": ["block_number"]},
            codec={"transfers": {"data": "ZSTD"}},
            skip_index={
                "transfers": [
                    ClickHouseSkipIndex(
                        name="log_addr_idx",
                        val="address",
                        type_="bloom_filter(0.01)",
                        granularity=1,
                    ),
                    ClickHouseSkipIndex(
                        name="from_addr_idx",
                        val="from",
                        type_="bloom_filter(0.01)",
                        granularity=1,
                    ),
                    ClickHouseSkipIndex(
                        name="to_addr_idx",
                        val="to",
                        type_="bloom_filter(0.01)",
                        granularity=1,
                    ),
                ]
            },
        ),
    )

    config = cc.Config(
        project_name="my_project",
        description="My description",
        pipelines={
            "my_pipeline": cc.Pipeline(
                provider=provider,
                writer=writer,
                steps=[
                    cc.Step(
                        name="decode_transfers",
                        kind=StepKind.EVM_DECODE_EVENTS,
                        config=EvmDecodeEventsConfig(
                            event_signature="Transfer(address indexed from, address indexed to, uint256 amount)",
                            output_table="transfers",
                        ),
                    ),
                    cc.Step(
                        name="join_data",
                        kind="join_data",
                    ),
                    cc.Step(
                        name="cast_timestamp",
                        kind=StepKind.CAST,
                        config=CastConfig(
                            table_name="transfers",
                            mappings={"block_timestamp": pa.int64()},
                        ),
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
    parser = argparse.ArgumentParser(description="example")

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

    asyncio.run(main(args.provider, url))
