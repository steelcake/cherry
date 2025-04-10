from typing import Any, Dict, Optional
from cherry_core import ingest
from cherry_etl import config as cc
import polars as pl
import logging

logger = logging.getLogger(__name__)


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    df = data["instructions"]

    processed_df = df.filter(
        pl.col("program_id") == "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
    )

    return {"instructions": processed_df}


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    from_block: int = 0,
    to_block: Optional[int] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    query = ingest.Query(
        kind=ingest.QueryKind.SVM,
        params=ingest.svm.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=True,
            fields=ingest.svm.Fields(
                instruction=ingest.svm.InstructionFields(
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
                    rest_of_accounts=True,
                    data=True,
                    error=True,
                    is_committed=True,
                    has_dropped_log_messages=True,
                ),
            ),
        ),
    )

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(kind=cc.StepKind.BASE58_ENCODE, config=cc.Base58EncodeConfig()),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
        ],
    )
