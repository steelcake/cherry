import logging
from typing import Any, Dict, Optional

import polars as pl
import pyarrow as pa
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)

TABLE_NAME = "blocks"


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    out = data["blocks"]

    return {"blocks": out}


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
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    number=True,
                    hash=True,
                    parent_hash=True,
                    nonce=True,
                    logs_bloom=True,
                    transactions_root=True,
                    state_root=True,
                    receipts_root=True,
                    miner=True,
                    difficulty=True,
                    total_difficulty=True,
                    extra_data=True,
                    size=True,
                    gas_limit=True,
                    gas_used=True,
                    timestamp=True,
                    uncles=True,
                    base_fee_per_gas=True,
                    withdrawals_root=True,
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
