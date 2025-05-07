# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/svm/instructions.py --from_block 330447751 --to_block 330447751

# After run, you can see the result in the database:
# duckdb data/instructions.db
# SELECT * FROM decoded_instructions LIMIT 3;

import argparse
import asyncio
import logging
import os
from typing import Optional
from pathlib import Path
import duckdb
from cherry_core import ingest

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.svm.instructions")

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    if to_block is None:
        logger.info(f"starting to ingest from block {from_block}")
    else:
        logger.info(f"starting to ingest from block {from_block} to {to_block}")

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
    # Hardcoded values for the example
    program_id = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    instruction_signature = InstructionSignature(
        discriminator="0x03",
        params=[
            ParamInput(
                name="Amount",
                param_type=DynType.U64,
            )
        ],
        accounts_names=["Source", "Destination", "Authority"],
    )

    # Create the pipeline using the blocks dataset
    pipeline = datasets.svm.make_instructions_pipeline(
        provider, writer, program_id, instruction_signature, from_block, to_block
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="instructions", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Connect to a persistent database file
    connection = duckdb.connect(f"{DATA_PATH}/instructions.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql("SELECT * FROM decoded_instructions LIMIT  3")
    logger.info(f"\n{data}")

    # Close the connection properly
    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instructions tracker")
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

    # Hardcoded values while there is only one provider
    provider_kind = ingest.ProviderKind.SQD
    provider_url = "https://portal.sqd.dev/datasets/solana-mainnet"

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(provider_kind, provider_url, from_block, to_block))
