# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/svm/jup_swaps.py --from_block 330447751 --to_block 330447751

# After run, you can see the result in the database:
# duckdb data/solana_swaps.db
# SELECT * FROM jup_swaps_decoded_instructions LIMIT 3;
# SELECT * FROM jup_swaps LIMIT 3;

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
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType, FixedArray


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.svm.jup_aggregator_swaps")

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
    dataset_name = "jup_swaps"
    program_id = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
    instruction_signature = InstructionSignature(
        discriminator="0xe445a52e51cb9a1d40c6cde8260871e2",
        params=[
            ParamInput(
                name="Amm",
                param_type=FixedArray(DynType.U8, 32),
            ),
            ParamInput(
                name="InputMint",
                param_type=FixedArray(DynType.U8, 32),
            ),
            ParamInput(
                name="InputAmount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="OutputMint",
                param_type=FixedArray(DynType.U8, 32),
            ),
            ParamInput(
                name="OutputAmount",
                param_type=DynType.U64,
            ),
        ],
        accounts_names=[],
    )

    # Create the pipeline using the blocks dataset
    pipeline = datasets.svm.make_instructions_pipeline(
        provider,
        writer,
        program_id,
        instruction_signature,
        from_block,
        to_block,
        dataset_name,
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="jup_swaps", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Connect to a persistent database file
    connection = duckdb.connect(f"{DATA_PATH}/solana_swaps.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # DB Operations - Create tables
    connection.sql(
        "CREATE OR REPLACE TABLE solana_amm AS SELECT * FROM read_csv('examples/using_datasets/svm/solana_amm.csv');"
    )
    connection.sql(
        "CREATE OR REPLACE TABLE solana_tokens AS SELECT * FROM read_csv('examples/using_datasets/svm/solana_tokens.csv');"
    )
    # DB Operations - Data Transformation
    connection.sql("""
        CREATE OR REPLACE TABLE jup_swaps AS            
            SELECT
                di.amm AS amm,
                sa.amm_name AS amm_name,
                case when di.inputmint > di.outputmint then it.token_symbol || '-' || ot.token_symbol
                    else ot.token_symbol || '-' || it.token_symbol
                    end as token_pair,
                    
                it.token_symbol as input_token,
                di.inputmint AS input_token_address,
                di.inputamount AS input_amount_raw,
                it.token_decimals AS input_token_decimals,
                di.inputamount / 10^it.token_decimals AS input_amount,
                
                ot.token_symbol as output_token,
                di.outputmint AS output_token_address,
                di.outputamount AS output_amount_raw,
                ot.token_decimals AS output_token_decimals,
                di.outputamount / 10^ot.token_decimals AS output_amount,

                di.block_slot AS block_slot,
                di.transaction_index AS transaction_index,
                di.instruction_address AS instruction_address,
                di.timestamp AS block_timestamp
            FROM jup_swaps_decoded_instructions di
            LEFT JOIN solana_amm sa ON di.amm = sa.amm_address
            LEFT JOIN solana_tokens it ON di.inputmint = it.token_address
            LEFT JOIN solana_tokens ot ON di.outputmint = ot.token_address;
                          """)
    data = connection.sql("SELECT * FROM jup_swaps LIMIT 3")
    logger.info(f"\n{data}")

    # DB Operations - Show table

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

    provider_kind = ingest.ProviderKind.SQD
    provider_url = "https://portal.sqd.dev/datasets/solana-mainnet"

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(provider_kind, provider_url, from_block, to_block))
