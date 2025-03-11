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
import pyarrow as pa
from typing import Dict, Optional
import argparse
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.compute as pc

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.absolute()


async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    blocks = blocks.rename_columns(["block_number", "block_timestamp"])
    out = transfers.join(blocks, keys="block_number")

    return {"transfers": out}


def get_start_block(output_dir: Path) -> int:
    try:
        transfers_dir = output_dir / "transfers"
        if not transfers_dir.exists():
            return 0

        parquet_files = list(transfers_dir.glob("**/*.parquet"))
        if not parquet_files:
            return 0

        last_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
        table = pq.read_table(last_file)

        if "block_number" in table.column_names:
            max_block = pc.max(table.column("block_number")).as_py()
            return max_block + 1

        return 0
    except Exception as e:
        logger.warning(f"Error reading start block: {e}")
        return 0


async def main(provider_kind: ingest.ProviderKind, url: Optional[str]):
    output_dir = SCRIPT_DIR / "data"
    from_block = get_start_block(output_dir)

    provider = cc.Provider(
        name="my_provider",
        config=ingest.ProviderConfig(
            kind=provider_kind,
            url=url,
            query=ingest.Query(
                kind=ingest.QueryKind.EVM,
                params=ingest.evm.Query(
                    from_block=from_block,  # Use the calculated start block
                    logs=[
                        ingest.evm.LogRequest(
                            address=[
                                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                            ],  # USDC contract
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
        kind=cc.WriterKind.PYARROW_DATASET,
        config=cc.PyArrowDatasetWriterConfig(
            output_dir=str(output_dir),
            partition_cols={"transfers": ["block_number"]},
            anchor_table="transfers",
            max_partitions=10000,
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
