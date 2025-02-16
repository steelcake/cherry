import hypersync
from hypersync import BlockSelection, HexOutput, HypersyncClient, TransactionField, LogField, BlockField, TraceField, Query, ClientConfig, StreamConfig, FieldSelection, ColumnMapping, DataType
import asyncio
from typing import Optional, cast 
import polars
from dataclasses import dataclass
import logging
import time

from .hypersync_config import load_config
from .athena import get_last_block_inserted
from .hypersync_util import prepare_and_insert_df
from .athena_schema import block_schema, transaction_schema, log_schema, trace_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class GetParquetParams:
    client: HypersyncClient
    from_block: int
    to_block: Optional[int]
    items_per_section: int
    database: str
    s3_path: str

async def get_parquet(params: GetParquetParams):
    query = Query(
        from_block=params.from_block,
        to_block=None if params.to_block is None else params.to_block + 1,
        join_mode=hypersync.JoinMode.JOIN_ALL,
        blocks=[BlockSelection()],
        field_selection=FieldSelection(
            log=[key for key in log_schema],
            block=[key for key in block_schema],
            transaction=[key for key in transaction_schema],
            trace=[key for key in trace_schema],
        ),
    )
    
    stream_conf = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        column_mapping=ColumnMapping(
            block={
                BlockField.DIFFICULTY: DataType.INTSTR,
                BlockField.TOTAL_DIFFICULTY: DataType.INTSTR,
                BlockField.SIZE: DataType.INTSTR,
                BlockField.GAS_LIMIT: DataType.INTSTR,
                BlockField.GAS_USED: DataType.INTSTR,
                BlockField.TIMESTAMP: DataType.INT64,
                BlockField.BASE_FEE_PER_GAS: DataType.INTSTR,
                BlockField.BLOB_GAS_USED: DataType.INTSTR,
                BlockField.EXCESS_BLOB_GAS: DataType.INTSTR,
                BlockField.SEND_COUNT: DataType.INTSTR,
            },
            transaction={
                TransactionField.GAS: DataType.INTSTR,
                TransactionField.GAS_PRICE: DataType.INTSTR,
                TransactionField.NONCE: DataType.INTSTR,
                TransactionField.VALUE: DataType.INTSTR,
                TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.INTSTR,
                TransactionField.MAX_FEE_PER_GAS: DataType.INTSTR,
                TransactionField.CHAIN_ID: DataType.INT64,
                TransactionField.CUMULATIVE_GAS_USED: DataType.INTSTR,
                TransactionField.EFFECTIVE_GAS_PRICE: DataType.INTSTR,
                TransactionField.GAS_USED: DataType.INTSTR,
                TransactionField.L1_FEE: DataType.INTSTR,
                TransactionField.L1_GAS_PRICE: DataType.INTSTR,
                TransactionField.L1_GAS_USED: DataType.INTSTR,
                TransactionField.L1_FEE_SCALAR: DataType.FLOAT64,
                TransactionField.GAS_USED_FOR_L1: DataType.INTSTR,
                TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.INTSTR,
            },
        ),
    )

    receiver = await params.client.stream_arrow(query, stream_conf)

    data = Data(params)

    chunk_start = query.from_block

    while True:
        logger.info(f"waiting for chunk from block {chunk_start}")

        t0 = time.time()

        res = await receiver.recv()
    
        t1 = time.time()

        if res is None:
            break

        logger.info(f"Waited {(t1-t0):.3f}secs for chunk")

        logger.info(f"Processing chunk {chunk_start}-{res.next_block}")

        data.append_data(res)

        t2 = time.time()

        logger.info(f"Finished processing chunk {chunk_start}-{res.next_block} in {(t2 - t1):.3f}secs")
        
        data.write_parquet_if_needed()

        t3 = time.time()

        logger.info(f"Finished conditionally wiring chunk {chunk_start}-{res.next_block} to s3 in {(t3 - t2):.3f}secs")

        chunk_start = res.next_block

    if not data.is_empty():
        data.write_parquet()

class Data:
    def __init__(self, params: GetParquetParams):
        self.from_block = params.from_block
        self.to_block = params.from_block 
        self.logs_df_list = []
        self.transactions_df_list = []
        self.blocks_df_list = []
        self.traces_df_list = []
        self.num_logs = 0
        self.num_transactions = 0
        self.num_blocks = 0
        self.num_traces = 0
        self.items_per_section = params.items_per_section 
        self.database = params.database
        self.s3_path = params.s3_path

    def is_empty(self) -> bool:
        return self.num_logs + self.num_transactions + self.num_blocks + self.num_traces == 0

    def append_data(self, res: hypersync.ArrowResponse):
        self.to_block = res.next_block
        logs_df = cast(polars.DataFrame, polars.from_arrow(res.data.logs))
        transactions_df = cast(polars.DataFrame, polars.from_arrow(res.data.transactions))
        blocks_df = cast(polars.DataFrame, polars.from_arrow(res.data.blocks))
        traces_df = cast(polars.DataFrame, polars.from_arrow(res.data.traces))

        block_timestamp_df = blocks_df.select([
            polars.col("number").alias("block_number"),
            polars.col("timestamp").alias("block_timestamp"),
        ])

        self.num_logs += logs_df.height
        self.num_transactions += transactions_df.height
        self.num_blocks += blocks_df.height
        self.num_traces += traces_df.height

        if logs_df.height > 0:
            logs_df = logs_df.join(block_timestamp_df, on="block_number")
            self.logs_df_list.append(logs_df)

        if transactions_df.height > 0:
            transactions_df = transactions_df.join(block_timestamp_df, on="block_number")
            self.transactions_df_list.append(transactions_df)

        if blocks_df.height > 0:
            blocks_df = blocks_df.with_columns(
                blocks_df.get_column("number").alias("block_number"),
                blocks_df.get_column("timestamp").alias("block_timestamp"),
            )
            self.blocks_df_list.append(blocks_df)

        if traces_df.height > 0:
            traces_df = traces_df.join(block_timestamp_df, on="block_number")
            self.traces_df_list.append(traces_df)

    def write_parquet_if_needed(self):
        logs = self.num_logs >= self.items_per_section
        transactions = self.num_transactions >= self.items_per_section
        blocks = self.num_blocks >= self.items_per_section
        traces = self.num_traces >= self.items_per_section

        if logs or transactions or blocks or traces:
            self.write_parquet()

    def write_parquet(self):
        print(f"writing parquet from block {self.from_block} to block {self.to_block}")

        prepare_and_insert_df(
            self.logs_df_list,
            "logs",
            self.database,
            self.s3_path,
            log_schema,
        )
        prepare_and_insert_df(
            self.transactions_df_list,
            "transactions",
            self.database,
            self.s3_path,
            transaction_schema,
        )
        prepare_and_insert_df(
            self.traces_df_list,
            "traces",
            self.database,
            self.s3_path,
            trace_schema,
        )
        # write blocks last
        prepare_and_insert_df(
            self.blocks_df_list,
            "blocks",
            self.database,
            self.s3_path,
            block_schema,
        )

        # reset data
        self.from_block = self.to_block
        self.logs_df_list = []
        self.transactions_df_list = []
        self.blocks_df_list = []
        self.traces_df_list = []
        self.num_logs = 0
        self.num_transactions = 0
        self.num_blocks = 0
        self.num_traces = 0

async def main():
    # config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    config_path = "src/config.json"

    config = load_config(config_path)

    client = HypersyncClient(ClientConfig(
        url=config.hypersync_url,
        bearer_token=config.hypersync_api_key,
    ))

    last_block = get_last_block_inserted(config.database, "blocks")

    if last_block > 0:
        config.from_block = last_block+1 if last_block+1 > config.from_block else config.from_block

    print(f"syncing all raw data from block {config.from_block} to block {config.to_block}") 

    await get_parquet(GetParquetParams(
        client=client,
        from_block=config.from_block,
        to_block=config.to_block,
        items_per_section=config.items_per_section,
        database=config.database,
        s3_path=config.s3_path,
    ))

def ingest_core_data():
    asyncio.run(main())

if __name__ == '__main__':
    asyncio.run(main())
