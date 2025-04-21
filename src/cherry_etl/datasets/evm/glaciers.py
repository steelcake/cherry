import logging
from typing import Optional

import pyarrow as pa
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    abi_db_path: str,
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
                kind=cc.StepKind.GLACIERS_EVENTS,
                config=cc.GlaciersEventsConfig(
                    abi_db_path=abi_db_path,
                ),
            ),
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )
