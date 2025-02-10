import hypersync
from hypersync import BlockField,TransactionField, ColumnMapping, HexOutput, HypersyncClient, LogField, Query, ClientConfig, LogSelection, StreamConfig, FieldSelection
import asyncio
from typing import List, Optional, cast, Dict
import polars
from dataclasses import dataclass
import sys
from slugify import slugify
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

from .hypersync_config import load_config
from .athena import get_last_block_inserted
from .hypersync_util import prepare_and_insert_df

@dataclass
class GetParquetParams:
    client: HypersyncClient
    column_mapping: Dict[str, hypersync.DataType]
    event_name: str
    addresses: Optional[List[str]]
    topics: Optional[List[List[str]]]
    signature: str
    contract_addr_list: Optional[List[polars.Series]]
    from_block: int
    to_block: Optional[int]
    items_per_section: int
    database: str
    s3_path: str
    get_txns: Optional[str]

async def get_parquet(params: GetParquetParams):
    topic0 = hypersync.signature_to_topic0(params.signature)
    topics: List[List[str]] = [[topic0]] 

    assert params.topics is None or len(params.topics) <= 3

    if params.topics is not None:
        for t in params.topics:
            topics.append(t)

    query = Query(
        from_block=params.from_block,
        to_block=None if params.to_block is None else params.to_block + 1,
        logs=[LogSelection(
            address=params.addresses,
            topics=topics,
        )],
        field_selection=FieldSelection(
            log=[e.value for e in LogField],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            # block=[e.value for e in BlockField],
            transaction=[TransactionField.HASH, TransactionField.FROM ],
        ),
    )

    logger.info(f"using query={query}")
    
    stream_conf = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        event_signature=params.signature,
        column_mapping=ColumnMapping(
            decoded_log=params.column_mapping,
            block={
                BlockField.TIMESTAMP: hypersync.DataType.INT64,
            }
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
        
        if data.num_logs > params.items_per_section:
            data.write_parquet()

        t3 = time.time()

        logger.info(f"Finished conditionally wiring chunk {chunk_start}-{res.next_block} to s3 in {(t3 - t2):.3f}secs")

        chunk_start = res.next_block

    if data.num_logs > 0:
        data.write_parquet()

class Data:
    def __init__(self, params: GetParquetParams):
        self.event_name = params.event_name
        self.from_block = params.from_block
        self.to_block = params.from_block 
        self.logs_df_list = []
        self.num_logs = 0
        self.contract_addr_list = params.contract_addr_list
        self.database = params.database
        self.s3_path = params.s3_path
        self.get_txns = True if params.get_txns is not None else False

    def append_data(self, res: hypersync.ArrowResponse):
        self.to_block = res.next_block
        logs_df = cast(polars.DataFrame, polars.from_arrow(res.data.logs))
        decoded_logs_df = cast(polars.DataFrame, polars.from_arrow(res.data.decoded_logs))
        blocks_df = cast(polars.DataFrame, polars.from_arrow(res.data.blocks))
        transactions_df = cast(polars.DataFrame, polars.from_arrow(res.data.transactions))

        if len(decoded_logs_df) > 0:
            prefixed_decoded_logs_df = decoded_logs_df.rename(lambda n: "decoded_" + n)

            prefixed_blocks_df = blocks_df.rename(lambda n: "block_" + n)
            prefixed_transactions_df = transactions_df.rename(lambda n: "transaction_" + n)
            combined_logs_df = logs_df.hstack(prefixed_decoded_logs_df).join(prefixed_blocks_df, on="block_number")
            combined_logs_df = logs_df.hstack(prefixed_decoded_logs_df).join(prefixed_blocks_df, on="block_number")
            
            if self.get_txns:
                combined_logs_df = combined_logs_df.join(prefixed_transactions_df, on="transaction_hash")

            if self.contract_addr_list is not None:
                for filter in self.contract_addr_list:
                    combined_logs_df = combined_logs_df.filter(polars.col("address").is_in(filter))

            self.num_logs += combined_logs_df.height

            if combined_logs_df.height > 0:
                self.logs_df_list.append(combined_logs_df)
        else:
            print("no data received for the event")

    def write_parquet(self):
        event_table = slugify(self.event_name, separator="_")
        prepare_and_insert_df(
            self.logs_df_list,
            event_table,
            self.database,
            self.s3_path,
            None,
        )

        # reset data
        self.from_block = self.to_block
        self.logs_df_list = []
        self.num_logs = 0

def concat_df_and_write_parquet(directory_path: str, table_name: str, df_list: List[polars.DataFrame], data_from_block: int, data_to_block: int):
    if len(df_list) == 0:
        print(f"skipping writing empty parquet for {table_name}")
        return
    polars.concat(df_list).write_parquet(f"{directory_path}/{table_name}_{data_from_block}_{data_to_block}.parquet")

async def get_contract_addr_list(client: HypersyncClient, contract_identifier_signatures: List[str]) -> Optional[List[polars.Series]]:
    print("getting contract address lists based on identifier signatures")

    if len(contract_identifier_signatures) == 0:
        return None

    addr_series_list: List[polars.Series] = []

    for sig in contract_identifier_signatures:
        print(f"getting contract address list for {sig}")

        topic0 = hypersync.signature_to_topic0(sig)

        query = hypersync.Query(
            from_block=0,
            logs=[LogSelection(
                topics=[[topic0]]
            )],
            field_selection=FieldSelection(
                log=[LogField.ADDRESS]
            ),
        )

        stream_conf = StreamConfig(
            hex_output=HexOutput.PREFIXED,
        )

        receiver = await client.stream_arrow(query, stream_conf)

        data: List[polars.Series] = []

        while True:
            res = await receiver.recv()

            if res is None:
                break
            
            logs_df = cast(polars.DataFrame, polars.from_arrow(res.data.logs))

            if logs_df.height > 0:
                data.append(logs_df.get_column("address").unique())

            print(f"processed up to block {res.next_block}")

        if len(data) == 0:
            continue

        data_concat: polars.Series = polars.concat(data)

        addr_series_list.append(data_concat.unique())

    return addr_series_list

async def main():
    # config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    config_path = "src/config.json"

    config = load_config(config_path)

    client = HypersyncClient(ClientConfig(
        url=config.hypersync_url,
        bearer_token=config.hypersync_api_key,
    ))
    config_from_block = config.from_block

    contract_addr_list = await get_contract_addr_list(client, config.contract_identifier_signatures)

    for event in config.events:
        if event.active != 'y':
            print(f"Event {event.name} is set to inactive in config, skipping it..")
            continue
        sig = event.signature
        name = event.name
        get_txns = event.get_transactions

        print(f"processing {name}\nsignature is {sig}\ntopic0 is {hypersync.signature_to_topic0(sig)}")

        event_table = slugify(event.name, separator="_")

        last_block = get_last_block_inserted(config.database, event_table)

        config.from_block = last_block+1 if last_block+1 > config_from_block else config_from_block

        print(f"start block for this event is {config.from_block}")    
        await get_parquet(GetParquetParams(
                client=client,
                column_mapping=event.column_mapping,
                event_name=name,
                signature=sig,
                contract_addr_list=contract_addr_list, 
                from_block=config.from_block,
                to_block=config.to_block,
                items_per_section=config.items_per_section,
                database=config.database,
                s3_path=config.s3_path,
                get_txns=get_txns,
                addresses=event.addresses,
                topics=event.topics,
            ))

def ingest_decoded_events():
    asyncio.run(main())

if __name__ == '__main__':
    asyncio.run(main())

