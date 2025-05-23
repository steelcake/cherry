"""Estimate USD value for DEX trades

For demonstration purposes, we use a simple WETH/USDC pool as price oracle.
"""

import polars as pl

WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_USDC_POOL = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
WETH_DECIMALS = 18
USDC_DECIMALS = 6


def calculate_amount_usd(swaps: pl.DataFrame) -> pl.DataFrame:
    """Calculate USD amounts for trades using ETH price from WETH/USDC pool"""
    # Find WETH/USDC swaps with substantial USDC amount
    eth_price_df = swaps.filter(
        (pl.col("address").str.to_lowercase() == WETH_USDC_POOL)
        & (pl.col("amount0In") + pl.col("amount0Out") > 500 * 10**USDC_DECIMALS)
    )

    # Calculate ETH price (USD per ETH)
    price_df = eth_price_df.select(
        pl.col("block_timestamp"),
        # Calculate price from raw amounts
        pl.when(pl.col("amount0In") > 0)
        .then(
            # USDC in, WETH out: USDC amount / WETH amount
            (pl.col("amount0In") * (1.0 / (10.0**USDC_DECIMALS)))
            / (pl.col("amount1Out") * (1.0 / (10.0**WETH_DECIMALS)))
        )
        .otherwise(
            # WETH in, USDC out: USDC amount / WETH amount
            (pl.col("amount0Out") * (1.0 / (10.0**USDC_DECIMALS)))
            / (pl.col("amount1In") * (1.0 / (10.0**WETH_DECIMALS)))
        )
        .alias("eth_price"),
    )

    # Join prices to swaps using asof join
    return (
        swaps.sort("block_timestamp")
        .join_asof(
            price_df.sort("block_timestamp"),
            left_on="block_timestamp",
            right_on="block_timestamp",
            strategy="backward",
        )
        .with_columns(
            # Calculate USD amount using token amounts and token decimals
            # The actual decimal adjustment will happen in pipeline.py
            pl.when(pl.col("token_bought_address").str.to_lowercase() == WETH)
            .then(
                # For WETH tokens, multiply by ETH price
                (
                    pl.col("token_bought_amount")
                    * (1.0 / (10.0 ** pl.col("token_bought_decimals")))
                )
                * pl.col("eth_price")
            )
            .when(pl.col("token_sold_address").str.to_lowercase() == WETH)
            .then(
                # For WETH tokens, multiply by ETH price
                (
                    pl.col("token_sold_amount")
                    * (1.0 / (10.0 ** pl.col("token_sold_decimals")))
                )
                * pl.col("eth_price")
            )
            .when(pl.col("token_bought_address").str.to_lowercase() == USDC)
            .then(
                # For USDC tokens, just use the amount
                pl.col("token_bought_amount")
                * (1.0 / (10.0 ** pl.col("token_bought_decimals")))
            )
            .when(pl.col("token_sold_address").str.to_lowercase() == USDC)
            .then(
                # For USDC tokens, just use the amount
                pl.col("token_sold_amount")
                * (1.0 / (10.0 ** pl.col("token_sold_decimals")))
            )
            .otherwise(None)
            .alias("amount_usd"),
            pl.col("hash").alias("tx_hash"),
        )
    )
