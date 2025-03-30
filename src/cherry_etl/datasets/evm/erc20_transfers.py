import logging
from typing import Any, Dict, Optional

import polars as pl
import pyarrow as pa
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)

TABLE_NAME = "erc20_transfers"


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    transactions = data["transactions"]
    logs = data["logs"]

    # Join transactions and logs on transaction_hash
    joined = logs.join(
        transactions, left_on="transaction_hash", right_on="hash", how="inner"
    )

    # Select and rename columns for the new table
    out = joined.select(
        [
            pl.col("block_number"),
            pl.col("block_hash"),
            pl.col("transaction_index"),
            pl.col("log_index"),
            pl.col("transaction_hash"),
            pl.col("address").alias("erc20"),
            pl.col("from").alias("from_address"),
            pl.col("to").alias("to_address"),
            pl.col("value"),
        ]
    )

    return {"erc20_transfers": out}


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    from_block: int = 0,
    to_block: Optional[int] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=True,
            transactions=[ingest.evm.TransactionRequest()],
            logs=[
                ingest.evm.LogRequest(
                    topic0=[
                        # ERC20 transfer event signature hash - Transfer(address,address,uint256)
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                    ]
                )
            ],
            fields=ingest.evm.Fields(
                log=ingest.evm.LogFields(
                    transaction_hash=True, log_index=True, address=True
                ),
                transaction=ingest.evm.TransactionFields(
                    hash=True,
                    from_=True,
                    to=True,
                    value=True,
                    block_number=True,
                    block_hash=True,
                    transaction_index=True,
                ),
            ),
        ),
    )

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )
