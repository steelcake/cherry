from cherry_etl import config as cc
from cherry_core import ingest
import logging
from typing import Any, Dict, Optional
import polars as pl

logger = logging.getLogger(__name__)

TABLE_NAME = "address_appearances"


# filter traces by action type, extract address column and create relationship column
def process_by_action_type(
    traces: pl.DataFrame, type_: str, address_column: str, relationship: str
) -> pl.DataFrame:
    df = traces.filter(pl.col("type").eq(type_))
    return df.select(
        pl.col("block_number"),
        pl.col("block_hash"),
        pl.col("transaction_hash"),
        pl.col(address_column).alias("address"),
        pl.lit(relationship).alias("relationship"),
    )


# this function will be used in our custom step in order to process traces and convert them to
#  address appearances
def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    traces = data["traces"]

    bn = traces.get_column("block_number")
    logger.info(f"processing data from: {bn.min()} to: {bn.max()}")

    call_from = process_by_action_type(traces, "call", "from", "call_from")
    call_to = process_by_action_type(traces, "call", "to", "call_to")
    factory = process_by_action_type(traces, "create", "from", "factory")
    suicide = process_by_action_type(traces, "selfdestruct", "address", "suicide")
    suicide_refund = process_by_action_type(
        traces, "selfdestruct", "refund_address", "suicide_refund"
    )
    author = process_by_action_type(traces, "reward", "author", "author")
    create = process_by_action_type(traces, "create", "address", "create")

    out = pl.concat(
        [call_from, call_to, factory, suicide, suicide_refund, author, create]
    )

    return {"address_appearances": out}


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    from_block: int = 0,
    to_block: Optional[int] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    query = ingest.Query(
        # we want evm data
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            # select all traces since we need all
            traces=[ingest.evm.TraceRequest()],
            # select the fields we need, can think of this like the SELECT fields FROM table SQL statement.
            fields=ingest.evm.Fields(
                # select these fields from traces table
                trace=ingest.evm.TraceFields(
                    block_number=True,
                    block_hash=True,
                    transaction_hash=True,
                    type_=True,
                    from_=True,
                    to=True,
                    address=True,
                    author=True,
                    refund_address=True,
                ),
            ),
        ),
    )

    # main pipeline configuration object, this object will tell cherry how to run the etl pipeline
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            # run our custom step to process traces into address appearances
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
            # prefix hex encode all binary fields so it is easy to view later
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    return pipeline
