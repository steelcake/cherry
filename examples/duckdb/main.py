import duckdb
import pyarrow as pa
from cherry import config as cc
from cherry.config import (
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
from typing import Dict
import argparse

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)


async def get_start_block(conn: duckdb.DuckDBPyConnection) -> int:
    try:
        result = conn.execute("SELECT MAX(block_number) FROM transfers").fetchone()
        return result[0] or 0 if result else 0
    except Exception as e:
        logger.warning(f"Failed to get start block: {str(e)}")
        return 0


async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    blocks = blocks.rename_columns(["block_number", "block_timestamp"])
    out = transfers.join(blocks, keys="block_number")

    return {"transfers": out}


async def main(provider_kind: ingest.ProviderKind):
    # Initialize DuckDB connection
    conn = duckdb.connect('blockchain.db')
    
    # Get start block from existing data
    from_block = await get_start_block(conn)
    logger.info(f"Starting to ingest from block {from_block}")

    provider = cc.Provider(
        name="my_provider",
        config=ingest.ProviderConfig(
            kind=provider_kind,
            url="https://portal.sqd.dev/datasets/ethereum-mainnet"
            if provider_kind == ingest.ProviderKind.SQD
            else None,
            query=ingest.Query(
                kind=ingest.QueryKind.EVM,
                params=ingest.evm.Query(
                    from_block=from_block,
                    logs=[
                        ingest.evm.LogRequest(
                            address=["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],  # USDC contract
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

    # Create DuckDB writer
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckDBWriterConfig(
            connection=conn,
            anchor_table="transfers"
        ),
    )

    config = cc.Config(
        project_name="usdc_transfers",
        description="Track USDC transfers on Ethereum",
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
    parser = argparse.ArgumentParser(description="DuckDB Writer Example")

    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

    asyncio.run(main(args.provider)) 