# Getting Started

Cherry is a Python indexer library designed for building blockchain data pipelines.

This guide will walk you through the core ideas behind Cherry and how you can use it to power your blockchain applications.

## Cherry Pipelines

In many cases, you'll find yourself reusing the same pipeline structures. To make your life easier, Cherry includes several built-in pipelines created as examples.
However, understanding how to create and customize pipelines yourself gives you the flexibility to adapt them perfectly to your needs.

You can build Cherry pipelines by following these steps:

1. **Defining a Provider** - Cherry supports multiple providers for accessing raw blockchain data across EVM and Solana networks. You can build pipelines on any chain that your provider supports, and you can even integrate new providers by making them compatible with Cherry’s query interface.
2. **Querying** - Queries let you specify exactly which blockchain data your pipeline needs, such as blocks, transactions, logs, and more. You can also control which fields (columns) to retrieve from each table, minimizing unnecessary data fetching and making your pipelines more efficient.
3. **Transformation Steps** - Allows you apply transformation steps to shape and prepare it before writing to storage. You can use popular Python data processing engines like Polars, PyArrow, Pandas, DataFusion, or DuckDB. Built-in transformations steps make it easy to:
    - Cast types
    - ABI/IDL decode data
    - Validate records
    - Encode data into hex or base58 strings
    - Join columns from other tables together.,

4. **Write to Database** - Cherry allows you to write your processed data to a variety of output formats or databases. This flexibility makes it easy to fit Cherry into your existing data stack or experiment with new storage solutions. It includes support for:
    - ClickHouse
    - Apache Iceberg
    - Delta Lake
    - DuckDB
    - Arrow Datasets
    - Parquet files

## Defining a Provider

Before querying blockchain data, you must define a Provider — the component responsible for connecting to a blockchain indexer or node.

In Cherry, a provider is configured through a ProviderConfig object, where you specify connection details, retry policies, and performance tuning options.
Providers are classified by their kind, depending on which network or service you're interacting with.

### Supported providers

Cherry currently supports the following providers (ProviderKind):
- sqd — Connects to the [SQD network](https://docs.sqd.ai/subsquid-network/overview/), a decentralized network, serving historical data for [EVM and Solana compatible chains]().
- hypersync — [Envio's hypersync](https://docs.envio.dev/docs/HyperSync/overview) is a high-speed, high-performance blockchain data retrieval, [supporting several EVM networks](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks).
- yellowstone_grpc — Connects to a Yellowstone GRPC endpoint, primarily for Solana (SVM) data access.

### Provider Configuration

You configure a provider using the [ProviderConfig class](https://github.com/steelcake/cherry-core/blob/f21f38454444fce153b44fec836eb45e6a4a3f0e/python/cherry_core/ingest/__init__.py#L27). Examples:

```python
from cherry_core import ingest
solana_provider = ProviderConfig(
        kind=ProviderKind.SQD,
        url="https://portal.sqd.dev/datasets/solana-mainnet",
    )

evm_hypersync_provider = ProviderConfig(
        kind=ProviderKind.HYPERSYNC,
        url="https://eth.hypersync.xyz",
    )

evm_sqd_provider = ProviderConfig(
        kind=ProviderKind.SQD,
        url="https://portal.sqd.dev/datasets/ethereum-mainnet",
    )
```

## Querying

The Query struct defines what data to fetch from a provider, how to filter it, and which fields to return. Queries are specific to a blockchain type (QueryKind), and can be either:

- evm (for Ethereum and compatible chains) or
- svm (for Solana Virtual Machine chains).

Each query consists of filters (to select subsets of data and tables) and field selectors (to specify what columns should be included in the response for each table).

A basic query contains:
```python
evm_query = Query(
    kind=evm.QueryKind.EVM
    params=evm.Query(
        from_block=from_block,                       # Required: Starting block number
        to_block=to_block,                           # Optional: Ending block number
        include_all_blocks=True,                     # Optional: Weather to include blocks with no matches in the tables request
        transactions=[transactions]                  # Optional: List of filters for specific transactions
        logs=[logs]                                  # Optional: List of filters for specific logs
        traces=[traces]                              # Optional: List of filters for specific traces
        fields=evm_field_selection                   # Required: Which fields (columns) to return on each table
    )
)

svm_query = Query(
    kind=QueryKind.SVM,
    params=svm.Query(
        from_block=from_block,                      # Required: Starting block number
        to_block=to_block,                          # Optional: Ending block number
        include_all_blocks=True,                    # Optional: Weather to include blocks with no matches in the tables request
        transactions= [transactions]                # Optional: List of filters for specific transactions
        instructions=[instructions]                 # Optional: List of filters for specific instructions
        logs=[logs]                                 # Optional: List of filters for specific logs
        balances=[balances]                         # Optional: List of filters for specific balances
        token_balances= [token_balances]            # Optional: List of filters for specific token balances
        rewards=[rewards]                           # Optional: List of filters for specific rewards
        fields=svm_field_selection                  # Required: Which fields (columns) to return on each table
    )
)
```

### Field Selection

Field selection lets you precisely choose which data fields (columns) to retrieve, rather than fetching entire objects. This minimizes bandwidth usage and speeds up processing.
All fields default to `False` and must be explicitly enabled when needed.

The full list of fields are defined in [evm](https://github.com/steelcake/cherry-core/blob/main/python/cherry_core/ingest/evm.py) and [svm](https://github.com/steelcake/cherry-core/blob/main/python/cherry_core/ingest/svm.py) field classes. Here is a simplified selection:

```python
evm_field_selection = evm.Fields(
    block=evm.BlockFields(
        number=True,
        hash=True,
        timestamp=True,
        ...
    ),
    transactions=evm.TransactionFields(
        from_=True,
        to=True,
        gas_price=True,
        hash=True,
        input=True,
        value=True,
        ...
    )
    log=evm.LogFields(
        address=True,
        data=True,
        topic0=True,
        log_index=True,
        ...
    ),
    trace=evm.TraceFields(
        type_=True,
        from_=True,
        to=True,
        address=True,
        author=True,
        ...
    ),
)

svm_field_selection = svm.Fields(
    block=svm.BlockFields(
        slot=True,
        hash=True,
        timestamp=True,
        ...
    ),
    instruction=svm.InstructionFields(
        block_hash=True,
        transaction_index=True,
        instruction_address=True,
        program_id=True,
        rest_of_accounts=True,
        data=True,
        ...
    ),
    transactions=svm.TransactionFields(
        block_slot=True,
        block_hash=True,
        transaction_index=True,
        signature=True,
        ...
    )
    log=svm.LogFields(
        log_index=True,
        instruction_address=True,
        program_id=True,
        kind=True,
        message=True,
        ...
    ),
    balance=svm.BalanceFields(
        account=True,
        pre=True,
        post=True,
        ...
    ),
    token_balance=svm.TokenBalanceFields(
        account=True,
        pre_mint=True,
        post_mint=True,
        pre_program_id=True,
        post_program_id=True,
        ...
    ),
    reward=svm.RewardFields(
        pubkey=True,
        lamports=True,
        post_balance=True,
        reward_type=True,
        commission=True,
        ...
    ),

)
```

### Filtering especific data

For both EVM and SVM queries, the main query objects enable fine-grained filtering of specific fields through `TransactionRequest`, `LogRequest`, `TraceRequest`, `InstructionRequest`, and others. Each request individually filters for a subset of rows in the tables. You can combine multiple requests to build complex queries tailored to your needs. w

Tables must be explicitly included either through a dedicated request or by using an `include_[table] parameter. The returned tables follow the field selection based on what’s been specified.

The full list of filters are defined in [evm](https://github.com/steelcake/cherry-core/blob/main/python/cherry_core/ingest/evm.py) and [svm](https://github.com/steelcake/cherry-core/blob/main/python/cherry_core/ingest/svm.py) request classes. Here some examples that can be made:

```python
# Combined of 2 requests 
# The first request will return a transaction table including all transactions in the block range, regardless of it's content
# The second request retrives a logs table where the topic0 match the transfer event signature + a traces tables from transactions that emited the transfer event.

transactions=[evm.TransactionRequest()], # Include all transactions in the transactions table for the block range
logs=[
    evm.LogRequest(
        topic0=["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] # ERC20 transfer event
        include_traces=True, # Include traces in the transactions table that matches the topic0 filter
    )
],
```

```python
# Retrieves  a instruction table where the program_id is either the Jupiter or Orca Whirlpool and the data starts with either of the discriminatos + a transactions table from transactions that executed those instructions + a logs table with the logs that those instructions emited.
instructions=[
    svm.InstructionRequest(
        program_id=["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"], # Include instructions that has the Jupiter or Orca program_id
        discriminator=["0xe445a52e51cb9a1d40c6cde8260871e2", "0x2b04ed0b1ac91e62"], # And the data starts with the discriminator "0xe445a52e51cb9a1d40c6cde8260871e2" or "0x2b04ed0b1ac91e62"
        include_transactions=True, # Also include the transactions that match the above 
        include_logs=True,
    )
],
```

## Transformation Steps

Cherry provides multiple ways to transform query results before writing them to storage. Transformation steps are built-in processing operations that are applied sequentially during pipeline execution. Each step has a type (called `kind`) and an associated configuration that defines its behavior.

Here’s an overview of how transformation steps work:

1. Select Step: Each transformation step defines series of operations. This can range from data validation, decoding, encoding, and joining data, to custom transformations.
2. Step Configuration: Each step is configured with configuration object that define its input params and modify it's behavior. For example, `EvmDecodeEventsConfig` requires an `event_signature` and can join raw and decoded data, while `HexEncodeConfig` allows you to select the tables you want encoded.
3. Process Flow: Steps are executed in the order they are provided. After each step, the data is updated, and the transformed data is passed to the next step in the pipeline.


### Custom Transformation Step

The Custom Step provides a powerful way to integrate user-defined processing logic into the pipeline. When using it, in the `CustomStepConfig`, you can specify a custom function (runner) to be executed on the data. This step offers maximum flexibility as you can define the exact transformations you need, tailoring them to specific use cases.

The structure of the `CustomStepConfig` has:
- runner: A callable function that processes the data. The function will take the data (a dictionary of tables, in polars' DataFrames format) and extra params passed in the context as input and returns the modified data.
- context: Optional context that can be passed to the runner function. This can be used for parameters such as filters, configurations, or any external context that the function may need.

Example Usage:

To add a custom step in the pipeline, you define the runner function and pass it into the CustomStepConfig. For example, if you want to filter instructions by a discriminator value, you can define a function like this:

```python
def process_data(
    data: Dict[str, pl.DataFrame], context: Optional[Dict[str, Any]] = None
) -> Dict[str, pl.DataFrame]:
    if context is None:
        return data

    discriminator = context.get("discriminator")

    if discriminator is not None and len(discriminator) > 8:
        df = data["instructions"]
        if isinstance(discriminator, str):
            discriminator = bytes.fromhex(discriminator.strip("0x"))
        else:
            pass
        processed_df = df.filter(pl.col("data").bin.starts_with(discriminator))
        data["instructions"] = processed_df

    return data
```
This process_data function takes the input data, checks the context for a discriminator value, and filters the instructions DataFrame based on that discriminator.

### Adding Steps to the Pipeline

Next, the steps can be added to the pipeline:

```python
from cherry_etl import config as cc

steps = [
    cc.Step(
        kind=cc.StepKind.CUSTOM,
        config=cc.CustomStepConfig(
            runner=process_data,
            context={"discriminator": instruction_signature.discriminator},
        ),
    ),
    cc.Step(
        kind=cc.StepKind.JOIN_SVM_TRANSACTION_DATA,
        config=cc.JoinSvmTransactionDataConfig(),
    ),
    cc.Step(
        kind=cc.StepKind.BASE58_ENCODE,
        config=cc.Base58EncodeConfig(),
    ),
]
```

## Write to Database

Once data has been transformed, Cherry provides flexible options to write the final output to different storage backends. The writing phase is handled by Writers, where each writer is responsible for persisting the data into a specific database or storage format.

Each writer has:
- A kind (which defines the storage backend).
- A configuration object that controls how and where the data will be written.

Cherry currently supports the following writers:
- iceberg
- clickhouse
- delta_lake
- pyarrow_dataset
- duckdb

#### Example of Creating a Writer
```python
from cherry_etl import config as cc

writer = cc.Writer(
    kind=cc.WriterKind.CLICKHOUSE,
    config=cc.ClickHouseWriterConfig(
        client=clickhouse_client,
        order_by={"transactions": ["block_number", "tx_index"]},
    )
)
```

## Running a Pipeline

With the above objects defined, we can now create our `Pipeline` object:
```python
from cherry_etl import config as cc

cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
)
```

And run it:
```python
from cherry_etl.pipeline import run_pipeline

run_pipeline(pipeline=pipeline)
```