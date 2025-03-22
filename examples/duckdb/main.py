import pyarrow as pa
from cherry_etl import config as cc
from cherry_etl.config import (
    CastByTypeConfig,
    StepKind,
    EvmDecodeEventsConfig,
    HexEncodeConfig,
    CastConfig,
)
from cherry_etl import run_pipelines, Context
from cherry_core import ingest
import logging
import os
import asyncio
from dotenv import load_dotenv
import traceback
from typing import Dict, Optional
import argparse
import polars
import duckdb

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)

db_path = "./data"


def get_start_block(con: duckdb.DuckDBPyConnection) -> int:
    try:
        res = con.sql("SELECT MAX(block_number) from transfers").fetchone()
        if res is not None:
            return int(res[0])
        else:
            return 0
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return 0


async def join_data(
    data: Dict[str, polars.DataFrame], _: cc.Step
) -> Dict[str, polars.DataFrame]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    blocks = blocks.select(
        polars.col("number").alias("block_number"),
        polars.col("timestamp").alias("block_timestamp"),
    )
    out = transfers.join(blocks, on="block_number")

    return {"transfers": out}


async def main(provider_kind: ingest.ProviderKind, url: Optional[str]):
    connection = duckdb.connect(database=db_path)

    from_block = get_start_block(connection)
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
                            include_blocks=True,
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
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
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
                        name="i256_to_i128",
                        kind=StepKind.CAST_BY_TYPE,
                        config=cc.CastByTypeConfig(
                            from_type=pa.decimal256(76, 0),
                            to_type=pa.decimal128(38, 0),
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
                        name="cast_by_type",
                        kind=StepKind.CAST_BY_TYPE,
                        config=CastByTypeConfig(
                            from_type=pa.decimal256(76, 0),
                            to_type=pa.decimal128(38, 0),
                            safe=True,
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
