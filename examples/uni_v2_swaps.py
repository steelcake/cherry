# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

################################################################################
# This example requires Plotly to be installed.
# To install it, run: pip install plotly
# Or with uv: uv pip install plotly
################################################################################

# You can run this script with:
# uv run examples/uni_v2_swaps.py
# After run, you can see the result in the database:
# duckdb data/uni_v2_swaps.db
# SELECT * FROM swaps_df LIMIT 3;
################################################################################
# Import dependencies

import asyncio
import pyarrow as pa
import polars as pl
from pathlib import Path
from typing import List, Dict, Any

import duckdb

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_core.ingest import (
    ProviderConfig,
    ProviderKind,
    QueryKind,
    Query as IngestQuery,
)
from cherry_core import evm_signature_to_topic0, get_token_metadata_as_table
from cherry_core.ingest.evm import (
    Query,
    Fields,
    TransactionFields,
    LogFields,
    BlockFields,
    LogRequest,
)

# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

################################################################################
# Configuration

# You can change the network by changing the URLs
PROVIDER_URLS = {
    ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}
RPC_PROVIDER_URL = "https://ethereum-rpc.publicnode.com"
FROM_BLOCK = 22000000
TO_BLOCK = 22200000
SWAP_EVENT_SIGNATURE = "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"
POOL_ADDRESS = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"
TOKEN0_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" # USDC
TOKEN1_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2" # WETH


################################################################################
# Main function

async def main():
    # Defining a Provider
    provider = ProviderConfig(
        kind=ProviderKind.HYPERSYNC,
        url=PROVIDER_URLS[ProviderKind.HYPERSYNC],
        stop_on_head=True,
    )

    event_signature = SWAP_EVENT_SIGNATURE
    topic0 = evm_signature_to_topic0(event_signature)
    # Querying
    query = IngestQuery(
        kind=QueryKind.EVM,
        params=Query(
            from_block=FROM_BLOCK,  # Required: Starting block number
            to_block=TO_BLOCK,  # Optional: Ending block number
            include_all_blocks=False,  # Optional: Weather to include blocks with no matches in the tables request
            fields=Fields(  # Required: Which fields (columns) to return on each table
                transaction=TransactionFields(  # Required: Transaction fields
                    block_number=True,
                    transaction_index=True,
                    hash=True,
                    from_=True,
                    to=True,
                ),
                log=LogFields(  # Required: Log fields
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
                block=BlockFields(
                    number=True,
                    timestamp=True,
                ),
            ),
            logs=[  # Optional: List of specific filters for logs
                LogRequest(
                    address=[POOL_ADDRESS],
                    topic0=[topic0],
                    include_transactions=True,
                    include_blocks=True,
                ),
            ],
        ),
    )
    
    token_df = get_token_df([TOKEN0_ADDRESS, TOKEN1_ADDRESS])

    # Transformation Steps
    steps = [
        # Decode the events
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=event_signature,
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        # Duckdb and Polars can't handle decimal256 values, so we need to limit the precision to 38 decimal places
        cc.Step(
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.decimal128(38, 0),
            ),
        ),
        # Join block and tx df to decoded logs
        cc.Step(
            kind=cc.StepKind.POLARS,
            config=cc.PolarsStepConfig(
                runner=join_block_and_tx_to_decoded_logs
            ),
        ),
        # Transform the decoded logs to a swaps df
        cc.Step(
            kind=cc.StepKind.POLARS,
            config=cc.PolarsStepConfig(
                runner=transform_to_swaps_df,
                context={
                    "token_df": token_df,
                },
            ),
        ),
        # Hex encode the data
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        ),
    ]

    # Write to Database
    connection = duckdb.connect("data/uni_v2_swaps.db")
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )



    await run_pipeline(pipeline_name="uni_v2_swaps", pipeline=pipeline)

    connection.close()
    plot_swaps_df()

################################################################################
# Get token metadata helper function

def get_token_df(token_address: List[str]) -> pl.DataFrame:
    try:
        token_metadata = get_token_metadata_as_table( # this helper function is from cherry-core package
            RPC_PROVIDER_URL,
            token_address,
        )
        token_df = pl.from_arrow(token_metadata)
    except Exception as e:
        print(f"Error getting token metadata for {token_address}: {e}")

    assert isinstance(token_df, pl.DataFrame), "token_df must be a DataFrame"

    token_df = token_df.select(
        pl.col("address").alias("address"),
        pl.col("name").alias("name"),
        pl.col("symbol").alias("symbol"),
        pl.col("decimals").alias("decimals"),
    )

    return token_df

################################################################################
# Transformation functions

def join_block_and_tx_to_decoded_logs(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    # data dict here contains all tables fetched from the provider (logs that were decoded into decoded_logs, blocks, transactions)
    logs = data["logs"]
    blocks = data["blocks"]
    max_block = blocks["number"].max()
    event_count = logs.shape[0]
    print(f"Running until block: {max_block}, event_count: {event_count}") # print in the console to see the progress

    swap_logs_df = data["decoded_logs"].select(
        pl.col("address").alias("pool_address"), # renaming to not conflict with the "address" in token df
        pl.exclude(["data", "topic0", "topic1", "topic2", "topic3"]) # exclude raw data and keep only the decoded data
    )
    transaction_df = data["transactions"].select(
        pl.col("hash").alias("transaction_hash"), # renaming to not create a new column when joining
        pl.col("to").alias("tx_to"), # renaming not to conflict with the "to" field in the event signature
        pl.col("from").alias("tx_from"), # renaming for better readability
    )
    swap_logs_df = swap_logs_df.join(data["blocks"], left_on="block_number", right_on="number", how="left")
    swap_logs_df = swap_logs_df.join(transaction_df, on="transaction_hash", how="left")
    new_data = {"swap_logs_df": swap_logs_df}
    # since we don't want to index raw tables, we will return only the swap_logs_df to the next step
    return new_data

def transform_to_swaps_df(data: Dict[str, pl.DataFrame], context: Any) -> Dict[str, pl.DataFrame]:
    # extract the token df from the context
    token_df = context["token_df"]
    # add the token0 and token1 columns to the swap_logs_df
    swap_logs_df = data["swap_logs_df"].with_columns(
        pl.lit(TOKEN0_ADDRESS).str.strip_prefix("0x").str.decode("hex").alias("token0"),
        pl.lit(TOKEN1_ADDRESS).str.strip_prefix("0x").str.decode("hex").alias("token1"),
    )

    # split the swap logs into which token is the input token, join the token df to get the token name and symbol and do some calculations
    token0_swap_df = (
        swap_logs_df.filter(pl.col("amount0In") - pl.col("amount0Out") > 0)  # token0 is the input token
        .join(token_df, left_on="token0", right_on="address", how="left") # without suffix are token0 columns
        .join(token_df, left_on="token1", right_on="address", how="left", suffix="_token1")
        .select([
            pl.col("timestamp").alias("block_timestamp"),
            pl.col("pool_address").alias("liquidity_pool"),
            pl.col("token0").alias("token_sold"),
            pl.col("symbol").alias("token_sold_symbol"),
            pl.col("amount0In").alias("amount_sold_raw"),
            (pl.col("amount0In") * (1.0 / (10.0 ** pl.col("decimals")))).alias("amount_sold"),
            pl.col("token1").alias("token_bought"),
            pl.col("symbol_token1").alias("token_bought_symbol"),
            pl.col("amount1Out").alias("amount_bought_raw"),
            (pl.col("amount1Out") * (1.0 / (10.0 ** pl.col("decimals_token1")))).alias("amount_bought"),
            pl.col("sender").alias("from"),
            pl.col("to").alias("to"),
            pl.col("tx_from").alias("tx_from"),
            pl.col("tx_to").alias("tx_to"),
            pl.col("transaction_hash").alias("tx_hash"),
            pl.col("block_number").alias("block_number"),
            pl.col("transaction_index").alias("tx_index"),
            pl.col("log_index").alias("log_index"),
        ])
    )
    token1_swap_df = (
        swap_logs_df.filter(pl.col("amount0In") - pl.col("amount0Out") < 0)  # token1 is the input token
        .join(token_df, left_on="token0", right_on="address", how="left") # without suffix are token0 columns
        .join( token_df, left_on="token1", right_on="address", how="left", suffix="_token1")
        .select([
            pl.col("timestamp").alias("block_timestamp"),
            pl.col("address").alias("liquidity_pool"),
            pl.col("token1").alias("token_sold"),
            pl.col("symbol_token1").alias("token_sold_symbol"),
            pl.col("amount1In").alias("amount_sold_raw"),
            (pl.col("amount1In") * (1.0 / (10.0 ** pl.col("decimals_token1")))).alias("amount_sold"),
            pl.col("token0").alias("token_bought"),
            pl.col("symbol").alias("token_bought_symbol"),
            pl.col("amount0Out").alias("amount_bought_raw"),
            (pl.col("amount0Out") * (1.0 / (10.0 ** pl.col("decimals")))).alias("amount_bought"),
            pl.col("sender").alias("from"),
            pl.col("to").alias("to"),
            pl.col("tx_from").alias("tx_from"),
            pl.col("tx_to").alias("tx_to"),
            pl.col("transaction_hash").alias("tx_hash"),
            pl.col("block_number").alias("block_number"),
            pl.col("transaction_index").alias("tx_index"),
            pl.col("log_index").alias("log_index"),
        ])
    )
    swap_df = token0_swap_df.vstack(token1_swap_df)
    return {"swaps_df": swap_df}

def plot_swaps_df():
    import duckdb
    import plotly.express as px
    import plotly.graph_objects as go

    ################################################################################
    # Configuration

    conn = duckdb.connect('data/uni_v2_swaps.db')

    ################################################################################
    # Swap Table Plot

    swaps_query = """
        SELECT 
            to_timestamp(block_timestamp) as block_timestamp,
            token_sold_symbol,
            amount_sold,
            token_bought_symbol,
            amount_bought,
            "from",
            "to",
            tx_from,
            tx_to,
            tx_hash
        FROM swaps_df
        LIMIT 1000
    """
    swaps_df = conn.execute(swaps_query).df()

    fig_01_table = go.Figure(data=[go.Table(
        header=dict(
            values=swaps_df.columns,
            align='left',
        ),
        cells=dict(
            values=[swaps_df[col] for col in swaps_df.columns],
            align='left',
        ),
        columnwidth=[500] * len(swaps_df.columns)
    )])

    fig_01_table.write_html("data/fig1.html", full_html=False, include_plotlyjs='cdn')
    # "full_html=False, include_plotlyjs='cdn'" exclude the plotlyjs source code to make the file smaller

    ################################################################################
    # Swap Count Plot

    swap_count_query = """
        SELECT 
            DATE_TRUNC('day', to_timestamp(block_timestamp)) as date,
            COUNT(*) as swap_count
        FROM swaps_df
        GROUP BY 1
        ORDER BY date
    """

    swap_count_df = conn.execute(swap_count_query).df()
    swap_count_fig = px.bar(swap_count_df, 
                x='date', 
                y='swap_count',
                title='Daily Uniswap V2 Swap Counts',
                labels={'date': 'Date', 'swap_count': 'Number of Swaps'})

    swap_count_fig.write_html("data/fig2.html", full_html=False, include_plotlyjs='cdn')
    # "full_html=False, include_plotlyjs='cdn'" exclude the plotlyjs source code to make the file smaller

    ################################################################################
    # Price and Volume Plots

    price_query = """
        WITH vol AS (
            SELECT 
                DATE_TRUNC('hour', to_timestamp(block_timestamp)) as hour,
                ABS(amount_sold) as volume_usd,
                ABS(amount_sold) / ABS(amount_bought) as price_usd
            FROM swaps_df
            WHERE token_sold = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            UNION ALL
            SELECT 
                DATE_TRUNC('hour', to_timestamp(block_timestamp)) as hour,
                ABS(amount_bought) as volume_usd,
                ABS(amount_bought) / ABS(amount_sold) as price_usd
            FROM swaps_df
            WHERE token_bought = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        )
        SELECT 
            hour,
            SUM(volume_usd) as volume_usd,
            AVG(price_usd) as price_usd
        FROM vol
        GROUP BY 1
        ORDER BY hour
    """
    price_df = conn.execute(price_query).df()
    price_fig = px.line(price_df, 
                x='hour', 
                y='price_usd',
                title='Hourly Uniswap V2 Price',
                labels={'hour': 'Time', 'price_usd': 'WETH Price (USD)'})

    price_fig.write_html("data/fig3.html", full_html=False, include_plotlyjs='cdn')
    # "full_html=False, include_plotlyjs='cdn'" exclude the plotlyjs source code to make the file smaller

    volume_fig = px.bar(price_df, 
                x='hour', 
                y='volume_usd',
                title='Hourly Uniswap V2 Volume',
                labels={'hour': 'Time', 'volume_usd': 'Volume (USD)'})

    volume_fig.write_html("data/fig4.html", full_html=False, include_plotlyjs='cdn')
    # "full_html=False, include_plotlyjs='cdn'" exclude the plotlyjs source code to make the file smaller

################################################################################
if __name__ == "__main__":
    asyncio.run(main())