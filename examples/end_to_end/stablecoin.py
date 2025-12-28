# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# # uv run examples/end_to_end/stablecoin.py --provider hypersync --from_block 15921958 --to_block 16000000

################################################################################
# Import dependencies

import argparse
import asyncio
import pyarrow as pa
import polars as pl
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from deltalake import DeltaTable
from deltalake import write_deltalake

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_core import evm_signature_to_topic0, get_token_metadata_as_table
from cherry_core.ingest import (
    ProviderKind,
    ProviderConfig,
    Query as IngestQuery,
    QueryKind,
)
from cherry_core.ingest.evm import (
    Query,
    Fields,
    BlockFields,
    TransactionFields,
    LogFields,
    LogRequest
)

# Create directories
DATA_PATH = str(Path.cwd() / "data/pyUSD")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


PROVIDER_URLS = {
    ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}
RPC_URL = "https://ethereum-rpc.publicnode.com"

EVENT_SIGNATURE = "Transfer(address indexed from, address indexed to, uint256 amount)"
TOKEN_ADDRESS = "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"

def create_protocol_table(current_time: int):
    protocol_table_path = f"{DATA_PATH}/protocol"
    if not DeltaTable.is_deltatable(protocol_table_path):
        protocol_data={
            "id": TOKEN_ADDRESS,
            "name": "pyUSD v1",
            "slug": "pyusd-v1",
            "schemaVersion": "1.0.0",
            "pipelineVersion": "1.0.0",
            "network": "ethereum",
            "type": "stablecoin",
            "exe_timestamp_utc": current_time
        }
        protocol_df = pl.DataFrame(protocol_data)
        write_deltalake(protocol_table_path, protocol_df)


def create_token_table(current_time: int):
    token_table_path = f"{DATA_PATH}/token"
    if not DeltaTable.is_deltatable(token_table_path):
        address = bytes.fromhex(TOKEN_ADDRESS.strip("0x"))
        token_metadata = get_token_metadata_as_table(
            RPC_URL,
            [TOKEN_ADDRESS],
        ).to_pydict()
        token_data={
            "id": TOKEN_ADDRESS,
            "address": [address],
            "name": token_metadata["name"][0],
            "symbol": token_metadata["symbol"][0],
            "decimal": token_metadata["decimals"][0],
            "exe_timestamp_utc": current_time
        }
        token_df = pl.DataFrame(token_data)
        write_deltalake(token_table_path, token_df)

def transformations(data: Dict[str, pl.DataFrame], context: Any) -> Dict[str, pl.DataFrame]:
    """
    Transform the decoded_logs DataFrame into the Transfer schema format.
    
    Args:
        decoded_logs: A polars DataFrame with the EVM transfer logs
        
    Returns:
        A polars DataFrame formatted according to the Transfer schema
    """
    
    decoded_logs_df = data["decoded_logs"]
    current_time = context["current_time"]
    token_df = pl.read_delta(f"{context["con"].data_uri}/token")
    
    # Create the transformation
    transfer_df = (
        decoded_logs_df
        .join(token_df, on="address", how="left")
        .select([
            pl.concat_str([pl.lit("0x"), pl.col("address").bin.encode("hex")]).alias("id"),
            pl.concat_str([pl.lit("0x"), pl.col("address").bin.encode("hex")]).alias("protocol"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("transaction_hash").alias("tx_hash"),
            pl.col("transaction_index").alias("tx_index"),
            pl.col("log_index"),
            pl.col("amount").alias("amount_raw"),
            (pl.col("amount") / 10**pl.col("decimal")).alias("amount"),
            pl.col("from_right").alias("tx_from"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp").cast(pl.Int64).alias("timestamp"),
            pl.lit(current_time).alias("exe_timestamp_utc"),
        ])
    )

    mint_df = (
        transfer_df
        .filter(pl.col("from") == pl.lit("0x0000000000000000000000000000000000000000").str.strip_prefix("0x").str.decode('hex'))
        .select([
            pl.col("id"),
            pl.col("protocol"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("tx_hash"),
            pl.col("tx_index"),
            pl.col("log_index"),
            pl.col("amount_raw").alias("amount_minted_raw"),
            pl.col("amount").alias("amount_minted"),
            pl.col("tx_from"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp"),
            pl.col("exe_timestamp_utc"),
        ])
    )
    
    burn_df = (
        transfer_df
        .filter(pl.col("to") == pl.lit("0x0000000000000000000000000000000000000000").str.strip_prefix("0x").str.decode('hex'))
        .select([
            pl.col("id"),
            pl.col("protocol"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("tx_hash"),
            pl.col("tx_index"),
            pl.col("log_index"),
            pl.col("amount_raw").alias("amount_burned_raw"),
            pl.col("amount").alias("amount_burned"),
            pl.col("tx_from"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp"),
            pl.col("exe_timestamp_utc"),
        ])
    )

    transfer_df = (
        transfer_df
        .filter(
            (pl.col("to") == pl.lit("0x0000000000000000000000000000000000000000").str.strip_prefix("0x").str.decode('hex'))
            | (pl.col("from") == pl.lit("0x0000000000000000000000000000000000000000").str.strip_prefix("0x").str.decode('hex'))
        )
    )

    data["transfer"] = transfer_df
    data["mint"] = mint_df
    data["burn"] = burn_df
    return data
    


################################################################################
# Main function

async def main(
    provider_kind: ProviderKind,
    provider_url: str,
    from_block: int,
    to_block: Optional[int],
):
    
    current_time = int(datetime.now(timezone.utc).timestamp())
    create_protocol_table(current_time)
    create_token_table(current_time)

    # Defining a Provider
    provider = ProviderConfig(
        kind=provider_kind,
        url=provider_url,
        stop_on_head=True,
    )

    # Write to Database
    writer=cc.Writer(
            kind=cc.WriterKind.DELTA_LAKE,
            config=cc.DeltaLakeWriterConfig(data_uri=DATA_PATH),
        )

    topic0 = evm_signature_to_topic0(EVENT_SIGNATURE)

    # Querying
    query = IngestQuery(
        kind=QueryKind.EVM,
        params=Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=False,
            logs=[LogRequest(
                address=[TOKEN_ADDRESS],
                topic0=[topic0],
                include_transactions=True,
                include_blocks=True
            )],
            fields=Fields(
                block=BlockFields(
                    number=True,
                    timestamp=True
                ),
                transaction=TransactionFields(
                    block_number=True,
                    transaction_index=True,
                    from_=True,
                    to=True,
                ),
                log=LogFields(
                    block_number=True,
                    block_hash=True,
                    transaction_index=True,
                    log_index=True,
                    transaction_hash=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    # Transformation Steps
    steps = [
        # Decode the events
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=EVENT_SIGNATURE,
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        # Handle decimal256 values
        cc.Step(
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.float64(),
            ),
        ),
        # Join the transaction data
        cc.Step(
            kind=cc.StepKind.JOIN_EVM_TRANSACTION_DATA,
            config=cc.JoinEvmTransactionDataConfig(),
        ),
        # Join the block data
        cc.Step(
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(),
        ),
        # Tranformations
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=transformations,
                context = {
                    "con": writer.config,
                    "current_time": current_time
                }
            ),
        ),
        # Hex encode the data
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        ),
    ]

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )
    await run_pipeline(pipeline_name="stablecoin", pipeline=pipeline)
    # Display results
    table = DeltaTable(f"{DATA_PATH}/{"decoded_logs"}").to_pyarrow_table()
    # Cast to DataFrame explicitly since we know it's a table
    df = pl.DataFrame(pl.from_arrow(table))
    print(f"Decoded Logs:\n{df}")



################################################################################
# CLI Argument Parser for starting and ending block
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instructions tracker")
    parser.add_argument(
        "--provider",
        choices=["hypersync", "sqd"],
        required=True,
        help="Specify the provider ('hypersync' or 'sqd')",
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

    url = PROVIDER_URLS[args.provider]
    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, url, from_block, to_block))