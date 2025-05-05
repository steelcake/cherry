# DEX Trades

This demo shows how use cherry and the [dexmetadata](https://github.com/elyase/dexmetadata) library  to create a local version of [Dune Analytics' dex.trades](https://docs.dune.com/data-catalog/curated/evm/DEX/dex-trades). The table has 25 columns divided into 3 main categories:

| Category | Fields |
|----------|---------|
| Metadata | `blockchain, project, version,  token_bought_symbol, token_sold_symbol, token_pair, token_bought_address, token_sold_address` |
| Swap Information | `block_month, block_date, block_time, block_number, token_bought_amount, token_sold_amount, token_bought_amount_raw, token_sold_amount_raw, taker, maker, project_contract_address, tx_hash, tx_from, tx_to, evt_index` |
| Prices | `amount_usd` |

Unlike [other](https://github.com/duneanalytics/spellbook/tree/main/dbt_subprojects/dex/models/trades) methods this example:

* uses a simple Python script that runs locally on your machine
* doesnt require fancy [dbt pipelines](https://github.com/duneanalytics/spellbook/tree/main/dbt_subprojects/dex/models/trades) setup / datawarehouse infrastructure / [customized nodes](https://github.com/shadow-hq/shadow-reth)
* is able to start streaming data without waiting for historical indexing to complete
* wont cost you lots of 💸🤑

## Running the Example


```bash
> uv pip install dexmetadata cherry-etl cherry-core
> uv run python examples/end_to_end/dextrades/main.py

┌────────────┬─────────┬─────────┬────────────┬────────────┬─────────────────────┬───────────────────┬─────────────────────┬───────────────────┬────────────┐
│ blockchain ┆ project ┆ version ┆ block_date ┆ block_time ┆ token_bought_symbol ┆ token_sold_symbol ┆ token_bought_amount ┆ token_sold_amount ┆ amount_usd │
│ ---        ┆ ---     ┆ ---     ┆ ---        ┆ ---        ┆ ---                 ┆ ---               ┆ ---                 ┆ ---               ┆ ---        │
│ str        ┆ str     ┆ str     ┆ str        ┆ str        ┆ str                 ┆ str               ┆ f64                 ┆ f64               ┆ f64        │
╞════════════╪═════════╪═════════╪════════════╪════════════╪═════════════════════╪═══════════════════╪═════════════════════╪═══════════════════╪════════════╡
│ ethereum   ┆ uniswap ┆ 2       ┆ 2023-08-26 ┆ 16:41:35   ┆ WETH                ┆ QUIRK             ┆ 0.001938            ┆ 21337.075664      ┆ 3.191601   │
│ ethereum   ┆ uniswap ┆ 2       ┆ 2023-08-26 ┆ 16:41:35   ┆ SHIELD              ┆ WETH              ┆ 974.337546          ┆ 0.55              ┆ 905.918754 │
│ ethereum   ┆ uniswap ┆ 2       ┆ 2023-08-26 ┆ 16:41:35   ┆ WETH                ┆ MBOT              ┆ 0.53                ┆ 131.744339        ┆ 872.976254 │
│ ethereum   ┆ uniswap ┆ 2       ┆ 2023-08-26 ┆ 16:41:35   ┆ PEPE                ┆ WETH              ┆ 7616.352669         ┆ 0.00693           ┆ 11.414436  │
│ ethereum   ┆ uniswap ┆ 2       ┆ 2023-08-26 ┆ 16:41:35   ┆ WETH                ┆ FLASH             ┆ 0.322445            ┆ 50000.0           ┆ 531.106439 │
└────────────┴─────────┴─────────┴────────────┴────────────┴─────────────────────┴───────────────────┴─────────────────────┴───────────────────┴────────────┘
```

## Checking the tables in storage
```bash
> duckdb data/dex_trades.db
show tables;
SELECT * FROM uni_v2_dex_trades LIMIT 3;
SELECT * FROM metadata LIMIT 3;
SELECT * FROM dex_trades LIMIT 3;
```

