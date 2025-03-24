from cherry_etl import config as cc
from cherry_etl import run_pipeline
from cherry_core import ingest
import logging
import os
import asyncio
from dotenv import load_dotenv
import traceback
from typing import Any, Dict, Optional
import argparse
import duckdb
import polars as pl

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

if not os.path.exists("data"):
    os.makedirs("data")

TABLE_NAME = "address_appearances"
DB_PATH = "./data/address_appearances"


def get_start_block(con: duckdb.DuckDBPyConnection) -> int:
    try:
        res = con.sql(f"SELECT MAX(block_number) from {TABLE_NAME}").fetchone()
        if res is not None:
            return int(res[0]) + 1
        else:
            return 0
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return 0


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


async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    start_block = get_start_block(connection)
    logger.info(f"starting to ingest from block {from_block}")

    from_block = max(start_block, from_block)

    if to_block is not None and from_block > to_block:
        logger.info(
            "skipping syncing data since the requested block range is behind the existing data"
        )
        return

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=provider_url,
        query=ingest.Query(
            kind=ingest.QueryKind.EVM,
            params=ingest.evm.Query(
                from_block=from_block,
                to_block=to_block,
                traces=[ingest.evm.TraceRequest()],
                fields=ingest.evm.Fields(
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
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        writer=writer,
        steps=[
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

    await run_pipeline(pipeline_name=TABLE_NAME, pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    connection = duckdb.connect(database=DB_PATH)

    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    data = connection.sql(
        f"SELECT address, COUNT(*) as appearances FROM {TABLE_NAME} GROUP BY address ORDER BY appearances DESC LIMIT 20"
    )
    logger.info(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Address appearances tracker")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )
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

    url = None
    if args.provider == ingest.ProviderKind.HYPERSYNC:
        url = "https://eth.hypersync.xyz"
    elif args.provider == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/ethereum-mainnet"

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, url, from_block, to_block))
