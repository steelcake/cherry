# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/end_to_end/jup_swap.py --from_block 330447751 --to_block 330447751

# After run, you can see the result in the database:
# duckdb data/solana_swaps.db
# SELECT * FROM jup_swaps_decoded_instructions LIMIT 3;
# SELECT * FROM jup_swaps LIMIT 3;
################################################################################
# Import dependencies

import argparse
import asyncio
from pathlib import Path
from typing import Optional

import duckdb

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType, FixedArray
from cherry_core.ingest import (
    ProviderConfig,
    ProviderKind,
    QueryKind,
    Query as IngestQuery,
)
from cherry_core.ingest.svm import (
    Query,
    Fields,
    InstructionFields,
    BlockFields,
    TransactionFields,
    InstructionRequest,
)


# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


################################################################################
# Main function


async def main(
    from_block: int,
    to_block: Optional[int],
):
    # Ensure to_block is not None, use from_block + 100 as default if it is
    actual_to_block = to_block if to_block is not None else from_block + 10

    # Defining a Provider
    provider = ProviderConfig(
        kind=ProviderKind.SQD,
        url="https://portal.sqd.dev/datasets/solana-mainnet",
    )

    # Querying
    query = IngestQuery(
        kind=QueryKind.SVM,
        params=Query(
            from_block=from_block,  # Required: Starting block number
            to_block=actual_to_block,  # Optional: Ending block number
            include_all_blocks=True,  # Optional: Weather to include blocks with no matches in the tables request
            fields=Fields(  # Required: Which fields (columns) to return on each table
                instruction=InstructionFields(
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    instruction_address=True,
                    program_id=True,
                    a0=True,
                    a1=True,
                    a2=True,
                    a3=True,
                    a4=True,
                    a5=True,
                    a6=True,
                    a7=True,
                    a8=True,
                    a9=True,
                    data=True,
                    error=True,
                ),
                block=BlockFields(
                    hash=True,
                    timestamp=True,
                ),
                transaction=TransactionFields(
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    signature=True,
                ),
            ),
            instructions=[  # Optional: List of specific filters for instructions
                InstructionRequest(
                    program_id=["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"],
                    discriminator=["0xe445a52e51cb9a1d40c6cde8260871e2"],
                    include_transactions=True,
                )
            ],
        ),
    )

    # Defining an Instruction Signature
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

    # Transformation Steps
    steps = [
        cc.Step(
            kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
            config=cc.SvmDecodeInstructionsConfig(
                instruction_signature=instruction_signature,
                hstack=True,
                allow_decode_fail=True,
                output_table="jup_swaps_decoded_instructions",
            ),
        ),
        cc.Step(
            kind=cc.StepKind.JOIN_SVM_TRANSACTION_DATA,
            config=cc.JoinSvmTransactionDataConfig(),
        ),
        cc.Step(
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(
                join_blocks_on=["hash"],
                join_left_on=["block_hash"],
            ),
        ),
        cc.Step(
            kind=cc.StepKind.BASE58_ENCODE,
            config=cc.Base58EncodeConfig(),
        ),
    ]

    # Write to Database
    connection = duckdb.connect("data/solana_swaps.db")
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )
    await run_pipeline(pipeline_name="jup_swaps", pipeline=pipeline)
    data = connection.sql("SELECT * FROM jup_swaps_decoded_instructions LIMIT 3")
    print(f"Decoded Instructions:\n{data}")

    # Post-pipeline Analytics
    connection.sql("""
        CREATE OR REPLACE TABLE solana_amm AS SELECT * FROM read_csv('examples/using_datasets/svm/solana_swaps/solana_amm.csv');
        CREATE OR REPLACE TABLE solana_tokens AS SELECT * FROM read_csv('examples/using_datasets/svm/solana_swaps/solana_tokens.csv');
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
    print(f"Dex Trades Jupiter Swaps:\n{data}")
    connection.close()


################################################################################
# CLI Argument Parser for starting and ending block
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

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(from_block, to_block))
