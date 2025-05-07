"""Pipeline configuration for DEX trades processing"""

import os
from pathlib import Path
from typing import Any, Dict

import polars as pl
import duckdb
import pyarrow as pa
from amount_usd import calculate_amount_usd
from dexmetadata import fetch
from dotenv import load_dotenv

import cherry_core
from cherry_etl import config as cc
from cherry_core import ingest

# Load environment variables
SCRIPT_DIR = Path(__file__).parent
dotenv_path = SCRIPT_DIR / ".env"
load_dotenv(dotenv_path=dotenv_path)

RPC_URL = os.getenv("RPC_URL")
if RPC_URL is None:
    raise ValueError("RPC_URL environment variable is not set")


def enrich_with_metadata(
    data: Dict[str, pl.DataFrame], connection: Any
) -> Dict[str, pl.DataFrame]:
    """Add token metadata for all pools"""
    # Get stored metadata
    if not isinstance(connection, duckdb.DuckDBPyConnection):
        raise ValueError("Writer must be configured with DuckDB connection")

    try:
        stored_metadata = connection.sql("SELECT * FROM metadata").pl()
    except duckdb.Error:
        stored_metadata = pl.DataFrame(
            schema={
                "address": pl.String,
                "token0_address": pl.String,
                "token0_name": pl.String,
                "token0_symbol": pl.String,
                "token0_decimals": pl.Int64,
                "token1_address": pl.String,
                "token1_name": pl.String,
                "token1_symbol": pl.String,
                "token1_decimals": pl.Int64,
                "is_valid": pl.Boolean,
                "protocol": pl.String,
                "identifier": pl.String,
                "chain_id": pl.Int64,
            }
        )
    # Join with decoded logs
    metadata_enriched = data["decoded_logs"].join(
        stored_metadata,
        on="address",
        how="left",
    )
    # Get rows with missing metadata
    missing_metadata = metadata_enriched.filter(pl.col("identifier").is_null()).select(
        data["decoded_logs"].columns
    )
    # Get new pool addresses
    new_pool_addresses = missing_metadata["address"].unique().to_list()

    if len(new_pool_addresses) > 0:
        # Ensure RPC_URL is set
        if RPC_URL is None:
            raise ValueError("RPC_URL environment variable is not set")
        # Fetch metadata with larger cache
        pools_list = fetch(
            new_pool_addresses,
            rpc_url=RPC_URL,
            batch_size=30,
            max_concurrent_batches=25,
            format="dict",
        )

        # Update metadata
        data["metadata"] = pl.DataFrame(pools_list).rename({"pool_address": "address"})

        # Join in the missing metadata
        missing_metadata = missing_metadata.join(
            data["metadata"],
            on="address",
            how="left",
        )

        # Union the rows with metadata from storage and newly fetched metadata
        data["uni_v2_dex_trades"] = pl.concat(
            [
                metadata_enriched.filter(pl.col("identifier").is_not_null()),
                missing_metadata,
            ]
        )
    else:
        data["uni_v2_dex_trades"] = metadata_enriched

    return data


def apply_dex_specific_logic(
    data: Dict[str, pl.DataFrame], dex: Any
) -> Dict[str, pl.DataFrame]:
    """Apply protocol-specific logic to determine token direction and amounts"""
    data["uni_v2_dex_trades"] = dex.transform(data["uni_v2_dex_trades"])
    return data


def estimate_usd_value(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    """Calculate USD amounts using WETH/USDC prices"""
    data["uni_v2_dex_trades"] = calculate_amount_usd(data["uni_v2_dex_trades"])
    return data


def format_dex_trades_table(
    data: Dict[str, pl.DataFrame], dex: Any
) -> Dict[str, pl.DataFrame]:
    """Format the final trades table according to the Dune schema"""
    data["dex_trades"] = data["uni_v2_dex_trades"].select(
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
    return data


# Pipeline configuration function
def create_pipeline(
    dex: Any,
    provider_kind: ingest.ProviderKind,
    provider_url: str,
    from_block: int,
    to_block: int,
    writer: cc.Writer,
    table_name: str,
):
    # Ensure writer is configured with DuckDB
    if not isinstance(writer.config, cc.DuckdbWriterConfig):
        raise ValueError("Writer must be configured with DuckDB connection")

    # Define query fields
    request_fields = ingest.evm.Fields(
        block=ingest.evm.BlockFields(number=True, timestamp=True),
        transaction=ingest.evm.TransactionFields(
            block_number=True, transaction_index=True, hash=True, from_=True, to=True
        ),
        log=ingest.evm.LogFields(
            block_number=True,
            transaction_index=True,
            log_index=True,
            address=True,
            data=True,
            topic0=True,
            topic1=True,
            topic2=True,
            topic3=True,
        ),
    )

    # Create provider config and query
    provider = ingest.ProviderConfig(
        kind=provider_kind, url=provider_url, buffer_size=1000
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                ingest.evm.LogRequest(
                    topic0=[cherry_core.evm_signature_to_topic0(dex.event_signature)],
                    include_blocks=True,
                    include_transactions=True,
                )
            ],
            fields=request_fields,
        ),
    )

    """Create the pipeline with Delta Lake writer"""
    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
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
            # Join transaction data to logs
            cc.Step(
                kind=cc.StepKind.JOIN_EVM_TRANSACTION_DATA,
                config=cc.JoinEvmTransactionDataConfig(),
            ),
            # Join block data to logs and transactions
            cc.Step(
                kind=cc.StepKind.JOIN_BLOCK_DATA,
                config=cc.JoinBlockDataConfig(),
            ),
            # Hex encode binary fields
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
            # Enrich with token metadata
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=enrich_with_metadata,
                    context=writer.config.connection,
                ),
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
