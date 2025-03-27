"""Pipeline configuration for DEX trades processing"""

import os
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pyarrow as pa
from amount_usd import calculate_amount_usd
from dexmetadata import fetch
from dotenv import load_dotenv

from cherry_etl import config as cc

# Load environment variables
SCRIPT_DIR = Path(__file__).parent
dotenv_path = SCRIPT_DIR / ".env"
load_dotenv(dotenv_path=dotenv_path)

RPC_URL = os.getenv("RPC_URL")
if RPC_URL is None:
    raise ValueError("RPC_URL environment variable is not set")


# Data processing functions
def join_raw_tables(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    """Join blocks, logs and transactions into a single table"""
    return {
        "raw_joined": data["decoded_logs"]
        .join(
            data["blocks"].select(
                pl.col("number").alias("block_number"),
                pl.col("timestamp").alias("block_timestamp"),
            ),
            on="block_number",
        )
        .join(
            data["transactions"].select(
                [
                    pl.col("block_number"),
                    pl.col("transaction_index"),
                    pl.col("from").alias("tx_from"),
                    pl.col("to").alias("tx_to"),
                    pl.col("hash").alias("tx_hash"),
                ]
            ),
            on=["block_number", "transaction_index"],
        )
    }


def enrich_with_metadata(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    """Add token metadata for all pools"""
    # Get unique pool addresses
    pool_addresses = data["raw_joined"]["address"].unique().to_list()

    # Ensure RPC_URL is set
    if RPC_URL is None:
        raise ValueError("RPC_URL environment variable is not set")

    # Fetch metadata with larger cache
    pools_list = fetch(
        pool_addresses,
        rpc_url=RPC_URL,
        batch_size=30,
        max_concurrent_batches=25,
        format="dict",
    )
    # Join metadata with trades
    return {
        "metadata_enriched": data["raw_joined"].join(
            pl.DataFrame(pools_list).rename({"pool_address": "address"}),
            on="address",
            how="left",
        )
    }


def apply_dex_specific_logic(
    data: Dict[str, pl.DataFrame], dex: Any
) -> Dict[str, pl.DataFrame]:
    """Apply protocol-specific logic to determine token direction and amounts"""
    return {"dex_processed": dex.transform(data["metadata_enriched"])}


def estimate_usd_value(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    """Calculate USD amounts using WETH/USDC prices"""
    return {
        **data,
        "dex_processed_with_prices": calculate_amount_usd(data["dex_processed"]),
    }


def format_dex_trades_table(
    data: Dict[str, pl.DataFrame], dex: Any
) -> Dict[str, pl.DataFrame]:
    """Format the final trades table according to the Dune schema"""

    return {
        "dex_trades": data["dex_processed_with_prices"].select(
            # Chain info
            blockchain=pl.lit(dex.blockchain),
            project=pl.lit(dex.project),
            version=pl.lit(dex.version),
            # Block info
            block_month=(
                ts := pl.from_epoch(pl.col("block_timestamp"), time_unit="s")
            ).dt.strftime("%Y-%m"),
            block_date=ts.dt.strftime("%Y-%m-%d"),
            block_time=ts.dt.strftime("%H:%M:%S"),
            block_number=pl.col("block_number"),
            # Token info
            token_bought_symbol=pl.col("token_bought_symbol"),
            token_sold_symbol=pl.col("token_sold_symbol"),
            token_pair=pl.col("token_pair"),
            # Store raw amounts
            token_bought_amount_raw=pl.col("token_bought_amount"),
            token_sold_amount_raw=pl.col("token_sold_amount"),
            # Calculate decimal-adjusted amounts using multiplication by reciprocal to avoid precision issues
            token_bought_amount=pl.col("token_bought_amount")
            * (1.0 / (10.0 ** pl.col("token_bought_decimals"))),
            token_sold_amount=pl.col("token_sold_amount")
            * (1.0 / (10.0 ** pl.col("token_sold_decimals"))),
            amount_usd=pl.col("amount_usd"),
            token_bought_address=pl.col("token_bought_address"),
            token_sold_address=pl.col("token_sold_address"),
            # Trade info
            taker=pl.col("sender"),
            maker=pl.col("to"),
            project_contract_address=pl.col("address"),
            tx_hash=pl.col("tx_hash"),
            tx_from=pl.col("tx_from"),
            tx_to=pl.col("tx_to"),
            evt_index=pl.col("log_index"),
            eth_price_usd=pl.col("eth_price"),
        )
    }


# Pipeline configuration function
def create_pipeline(dex, delta_path, provider, query, table_name):
    """Create the pipeline with Delta Lake writer"""
    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=cc.Writer(
            kind=cc.WriterKind.DELTA_LAKE,
            config=cc.DeltaLakeWriterConfig(data_uri=delta_path),
        ),
        steps=[
            # Decode Swap events
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=dex.event_signature,
                    output_table="decoded_logs",
                    hstack=True,
                ),
            ),
            # Cast numeric fields to proper types
            cc.Step(
                kind=cc.StepKind.CAST,
                config=cc.CastConfig(
                    table_name="decoded_logs",
                    mappings=dex.type_mappings,
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
            # Join raw tables
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(runner=join_raw_tables),
            ),
            # Hex encode binary fields
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
            # Enrich with token metadata
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(runner=enrich_with_metadata),
            ),
            # Apply DEX-specific logic
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=apply_dex_specific_logic,
                    context=dex,
                ),
            ),
            # Calculate USD values
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(runner=estimate_usd_value),
            ),
            # Format final output
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=format_dex_trades_table,
                    context=dex,
                ),
            ),
        ],
    )
