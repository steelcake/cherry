import argparse
import asyncio
import logging
import os
from typing import Optional

import duckdb
from cherry_core import ingest

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType, Variant, Field


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.svm.jup_aggregator_swaps")


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
                param_type=DynType.Struct([
                    Field(
                        name="slices",
                        element_type=DynType.Struct([
                            Field(
                                name="accountsType",
                                element_type=DynType.Enum([
                                    Variant("TransferHookA", None),
                                    Variant("TransferHookB", None),
                                    Variant("TransferHookReward", None),
                                    Variant("TransferHookInput", None),
                                    Variant("TransferHookIntermediate", None),
                                    Variant("TransferHookOutput", None),
                                    Variant("SupplementalTickArrays", None),
                                    Variant("SupplementalTickArraysOne", None),
                                    Variant("SupplementalTickArraysTwo", None),
                                ])
                            ),
                            Field(
                                name="length",
                                element_type=DynType.U8

                            )
                        ]),
                    ),
                ])
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
            "tokenVaultB",
            "tickArray0",
            "tickArray1",
            "tickArray2",
            "oracle",
        ],
    )

    # Create the pipeline using the blocks dataset
    pipeline = datasets.svm.instructions(
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
    connection = duckdb.connect("examples/datasets/svm/jup_swaps/jup_swaps.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # DB Operations - Create tables
    connection.sql(
        "CREATE OR REPLACE TABLE solana_tokens AS SELECT * FROM read_csv('examples/datasets/svm/jup_swaps/solana_tokens.csv');"
    )
    # DB Operations - Data Transformation
    data =connection.sql("""
            SELECT *
            FROM instructions;
                          """)
    # connection.sql("COPY jup_swaps TO 'jup_swaps.parquet' (FORMAT PARQUET)")
    # data = connection.sql("SELECT * FROM jup_swaps LIMIT 3")
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
