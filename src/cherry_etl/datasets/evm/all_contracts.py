import logging
from typing import Any, Dict, Optional

import polars as pl
import pyarrow as pa
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)

TABLE_NAME = "contracts"


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    data[TABLE_NAME] = data["traces"]
    data.pop("traces")
    return data


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
            include_all_blocks=False,
            traces=[ingest.evm.TraceRequest(type_=["create"])],
            fields=ingest.evm.Fields(
                trace=ingest.evm.TraceFields(
                    address=True, block_number=True, transaction_hash=True
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
