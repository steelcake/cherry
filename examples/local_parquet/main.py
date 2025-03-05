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
<<<<<<< HEAD
import pyarrow as pa
from typing import Dict
import argparse
=======
from dotenv import load_dotenv
import pyarrow as pa
from typing import Dict
import argparse
from pathlib import Path
>>>>>>> ec9330d (add local writer)

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)

<<<<<<< HEAD
=======
# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()

>>>>>>> ec9330d (add local writer)

async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    blocks = blocks.rename_columns(["block_number", "block_timestamp"])
    out = transfers.join(blocks, keys="block_number")

    return {"transfers": out}


async def main(provider_kind: ingest.ProviderKind):
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
                    from_block=0,  # Start from genesis for example
                    logs=[
                        ingest.evm.LogRequest(
<<<<<<< HEAD
                            address=[
                                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                            ],  # USDC contract
=======
                            address=["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],  # USDC contract
>>>>>>> ec9330d (add local writer)
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

    # Create writer with local parquet configuration
    writer = cc.Writer(
        kind=cc.WriterKind.LOCAL_PARQUET,
<<<<<<< HEAD
        config=cc.LocalParquetWriterConfig(output_dir="./data"),
=======
        config=cc.LocalParquetWriterConfig(
            output_dir=str(SCRIPT_DIR / "data")
        ),
>>>>>>> ec9330d (add local writer)
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
<<<<<<< HEAD
                            mappings={"block_timestamp": pa.int64()},
=======
                            mappings=[("block_timestamp", "Int64")],
>>>>>>> ec9330d (add local writer)
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
    parser = argparse.ArgumentParser(description="Local Parquet Example")

    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

<<<<<<< HEAD
    asyncio.run(main(args.provider))
=======
    asyncio.run(main(args.provider)) 
>>>>>>> ec9330d (add local writer)
