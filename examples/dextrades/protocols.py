"""
Contains implementations of specific DEX protocol logic for transforming swap events.
"""

from dataclasses import dataclass

import polars as pl
import pyarrow as pa


@dataclass
class UniswapV2:
    blockchain = "ethereum"
    project = "uniswap"
    version = "2"
    event_signature = "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"

    # Data type mappings for event parameters
    type_mappings = {
        "amount0In": pa.float64(),
        "amount1In": pa.float64(),
        "amount0Out": pa.float64(),
        "amount1Out": pa.float64(),
        "timestamp": pa.int64(),
    }

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply Uniswap V2-specific logic to determine trade direction and token amounts.
        For Uniswap V2:
        - If amount0In > 0, token0 is being sold (token1 is bought)
        - If amount1In > 0, token1 is being sold (token0 is bought)
        """
        return df.with_columns(
            # Determine if token0 is being sold (amount0In > 0) for internal use
            is_token0_sold := pl.col("amount0In") > 0,
            # Set token symbols based on trade direction
            pl.when(is_token0_sold)
            .then(pl.col("token1_symbol"))
            .otherwise(pl.col("token0_symbol"))
            .alias("token_bought_symbol"),
            pl.when(is_token0_sold)
            .then(pl.col("token0_symbol"))
            .otherwise(pl.col("token1_symbol"))
            .alias("token_sold_symbol"),
            # Set token addresses based on trade direction
            pl.when(is_token0_sold)
            .then(pl.col("token1_address"))
            .otherwise(pl.col("token0_address"))
            .alias("token_bought_address"),
            pl.when(is_token0_sold)
            .then(pl.col("token0_address"))
            .otherwise(pl.col("token1_address"))
            .alias("token_sold_address"),
            # Create token pair (dune uses alphabetical order)
            pl.concat_str(
                [pl.col("token0_symbol"), pl.lit("/"), pl.col("token1_symbol")]
            ).alias("token_pair"),
            # Calculate bought/sold amounts
            pl.when(is_token0_sold)
            .then(pl.col("amount1Out"))
            .otherwise(pl.col("amount0Out"))
            .alias("token_bought_amount"),
            pl.when(is_token0_sold)
            .then(pl.col("amount0In"))
            .otherwise(pl.col("amount1In"))
            .alias("token_sold_amount"),
            # Store decimals for bought/sold tokens
            pl.when(is_token0_sold)
            .then(pl.col("token1_decimals"))
            .otherwise(pl.col("token0_decimals"))
            .alias("token_bought_decimals"),
            pl.when(is_token0_sold)
            .then(pl.col("token0_decimals"))
            .otherwise(pl.col("token1_decimals"))
            .alias("token_sold_decimals"),
            pl.col("timestamp").alias("block_timestamp"),
            pl.col("hash").alias("tx_hash"),
            pl.col("from").alias("tx_from"),
            pl.col("to_right").alias("tx_to"),
        )
