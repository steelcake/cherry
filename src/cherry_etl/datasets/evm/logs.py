import logging
from typing import Optional, List

import pyarrow as pa
from cherry_core import ingest

from cherry_etl import config as cc

logger = logging.getLogger(__name__)


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    event_full_signature: str,
    from_block: int = 0,
    to_block: Optional[int] = None,
    address: Optional[List[str]] = [],
    topic0: Optional[List[str]] = [],
    topic1: Optional[List[str]] = [],
    topic2: Optional[List[str]] = [],
    topic3: Optional[List[str]] = [],
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    if not isinstance(address, list):
        raise TypeError("address must be a list")
    if not isinstance(topic0, list):
        raise TypeError("topic0 must be a list")
    if not isinstance(topic1, list):
        raise TypeError("topic1 must be a list")
    if not isinstance(topic2, list):
        raise TypeError("topic2 must be a list")
    if not isinstance(topic3, list):
        raise TypeError("topic3 must be a list")

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=False,
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    hash=True,
                    number=True,
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
            ),
            logs=[
                ingest.evm.LogRequest(
                    include_blocks=True,
                    address=address,
                    topic0=topic0,
                    topic1=topic1,
                    topic2=topic2,
                    topic3=topic3,
                )
            ],
        ),
    )

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=event_full_signature,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                ),
            ),
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
        ],
    )
