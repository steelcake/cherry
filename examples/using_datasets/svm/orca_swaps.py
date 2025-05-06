# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/svm/orca_swaps.py --from_block 330447757 --to_block 330447760

# After run, you can see the result in the database:
# duckdb data/solana_swaps.db
# SELECT * FROM orca_swaps_decoded_instructions LIMIT 3;
# SELECT * FROM orca_swaps LIMIT 3;
# SELECT * FROM orca_swaps_decoded_logs LIMIT 3;

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
from cherry_core.svm_decode import (
    InstructionSignature,
    ParamInput,
    DynType,
    Variant,
    Field,
    LogSignature,
)


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.svm.orca_swaps")

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
    dataset_name = "orca_swaps"
    program_id = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
    instruction_signature = InstructionSignature(
        discriminator="0x2b04ed0b1ac91e62",
        params=[
            ParamInput(
                name="amount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="otherAmountThreshold",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="sqrtPriceLimit",
                param_type=DynType.U128,
            ),
            ParamInput(
                name="amountSpecifiedIsInput",
                param_type=DynType.Bool,
            ),
            ParamInput(
                name="aToB",
                param_type=DynType.Bool,
            ),
            ParamInput(
                name="remainingAccountsInfo",
                param_type=DynType.Struct(
                    [
                        Field(
                            name="slices",
                            element_type=DynType.Option(
                                DynType.Struct(
                                    [
                                        Field(
                                            name="accountsType",
                                            element_type=DynType.Enum(
                                                [
                                                    Variant("TransferHookA", None),
                                                    Variant("TransferHookB", None),
                                                    Variant("TransferHookReward", None),
                                                    Variant("TransferHookInput", None),
                                                    Variant(
                                                        "TransferHookIntermediate", None
                                                    ),
                                                    Variant("TransferHookOutput", None),
                                                    Variant(
                                                        "SupplementalTickArrays", None
                                                    ),
                                                    Variant(
                                                        "SupplementalTickArraysOne",
                                                        None,
                                                    ),
                                                    Variant(
                                                        "SupplementalTickArraysTwo",
                                                        None,
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Field(name="length", element_type=DynType.U8),
                                    ]
                                )
                            ),
                        ),
                    ]
                ),
            ),
        ],
        accounts_names=[
            "tokenProgramA",
            "tokenProgramB",
            "memoProgram",
            "tokenAuthority",
            "whirlpool",
            "tokenMintA",
            "tokenMintB",
            "tokenOwnerAccountA",
            "tokenVaultA",
            "tokenOwnerAccountB",
        ],
    )

    log_signature = LogSignature(
        params=[
            ParamInput(
                name="whirlpool",
                param_type=DynType.FixedArray(DynType.U8, 32),
            ),
            ParamInput(
                name="a_to_b",
                param_type=DynType.Bool,
            ),
            ParamInput(
                name="pre_sqrt_price",
                param_type=DynType.U128,
            ),
            ParamInput(
                name="post_sqrt_price",
                param_type=DynType.U128,
            ),
            ParamInput(
                name="x",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="input_amount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="output_amount",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="input_transfer_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="output_transfer_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="lp_fee",
                param_type=DynType.U64,
            ),
            ParamInput(
                name="protocol_fee",
                param_type=DynType.U64,
            ),
        ],
    )

    # Create the pipeline using the blocks dataset
    pipeline = datasets.svm.make_instructions_and_logs_pipeline(
        provider,
        writer,
        program_id,
        instruction_signature,
        from_block,
        to_block,
        dataset_name,
        log_signature,
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="orca_swaps", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Connect to a persistent database file
    connection = duckdb.connect("data/solana_swaps.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # DB Operations - Create tables
    connection.sql(
        "CREATE OR REPLACE TABLE solana_tokens AS SELECT * FROM read_csv('examples/using_datasets/svm/solana_tokens.csv');"
    )
    # DB Operations - Data Transformation
    data = connection.sql("""
        CREATE OR REPLACE TABLE orca_swaps AS  
            SELECT
                di.program_id AS amm,
                'Orca Whirlpool' AS amm_name,
                'v2' AS amm_version,

                case when di.tokenMintB > di.tokenMintA then it.token_symbol || '-' || ot.token_symbol
                    else ot.token_symbol || '-' || it.token_symbol
                    end as token_pair,
                it.token_symbol as token_sold_symbol,
                di.tokenMintB as token_sold_address,
                dl.input_amount as token_sold_amount_raw,
                it.token_decimals as token_sold_decimals,
                dl.input_amount / 10^it.token_decimals as token_sold_amount,

                ot.token_symbol as token_bought_symbol,
                di.tokenMintA as token_bought_address,
                dl.output_amount as token_bought_amount_raw,
                ot.token_decimals as token_bought_decimals,
                dl.output_amount / 10^ot.token_decimals as token_bought_amount,
                          
                dl.lp_fee as lp_fee,
                dl.protocol_fee as protocol_fee,

                di.block_slot AS block_slot,
                di.transaction_index AS transaction_index,
                di.instruction_address AS instruction_address,
                di.timestamp AS block_timestamp,
                di.signature AS signature
            FROM orca_swaps_decoded_instructions di
            LEFT JOIN orca_swaps_decoded_logs dl 
                ON dl.kind = 'data'
                AND di.block_hash = dl.block_hash
                AND di.instruction_address = dl.instruction_address
                AND di.signature = dl.signature
                AND di.amount = dl.input_amount
            LEFT JOIN solana_tokens it ON di.tokenMintB = it.token_address
            LEFT JOIN solana_tokens ot ON di.tokenMintA = ot.token_address;
                          """)
    data = connection.sql("SELECT * FROM orca_swaps LIMIT 3")
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
