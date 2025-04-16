import logging
from typing import Any, Dict, Optional

import pyarrow as pa
import glaciers as gl
import polars as pl
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    logs_df = data["logs"]
    abi_db_path = "examples/database/glaciers/ethereum__events__abis.parquet"
    decoder_type = "log"

    # gl.set_config(field="decoder.output_hex_string_encoding", value=True)
    decoded_df = gl.decode_df(decoder_type, logs_df, abi_db_path)
    data["decoded_logs"] = pl.DataFrame(decoded_df)

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
            include_all_blocks=True,
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    hash=True,
                    timestamp=True,
                ),
                log=ingest.evm.LogFields(
                    address=True,
                    data=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    block_number=True,
                    block_hash=True,
                    transaction_hash=True,
                    transaction_index=True,
                    log_index=True,
                    removed=True,
                ),
                transaction=ingest.evm.TransactionFields(
                    from_=True,
                    to=True,
                    value=True,
                    status=True,
                    block_hash=True,
                    hash=True,
                ),
            ),
            logs=[ingest.evm.LogRequest()],
            transactions=[ingest.evm.TransactionRequest()],
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
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )
