from dataclasses import dataclass
import logging
import sys
import hypersync
from hypersync import (
    HypersyncClient, 
    ClientConfig,
    StreamConfig,
    HexOutput,
    Query,
    LogSelection,
    FieldSelection,
    LogField,
    BlockField,
    ColumnMapping,
    signature_to_topic0
)
import polars as pl
from typing import Dict, List, cast, Optional
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config

logger = logging.getLogger(__name__)

@dataclass
class GetParquetParams:
    client: HypersyncClient
    column_mapping: Dict[str, hypersync.DataType]
    event_name: str
    signature: str
    contract_addr_list: Optional[List[pl.Series]]
    from_block: int
    to_block: Optional[int]
    items_per_section: int

class HypersyncIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config
        logger.info(f"Initializing HypersyncIngester with URL: {config.data_source[0].url}")
        self.client = HypersyncClient(ClientConfig(
            url=config.data_source[0].url,
            bearer_token=config.data_source[0].api_key,
        ))
        self.events = {event.name: event for event in config.events}
    
    async def get_contract_addr_list(self, contract_identifier_signatures: List[str]) -> Optional[List[pl.Series]]:
        addr_series_list: List[pl.Series] = []
            
        for sig in self.config.contract_identifier_signatures:
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
            
            logger.info(f"Processing signature {sig}")

            try:
                stream_conf = StreamConfig(
                    hex_output=HexOutput.PREFIXED,
                )
                logger.info("Stream conf created")

                receiver = await self.client.stream_arrow(query, stream_conf)

                data: List[pl.Series] = []

                for i in range(10):
                    res = await receiver.recv()

                    logger.info(res)

                    if res is None:
                        break
                    
                    logs_df = cast(pl.DataFrame, pl.from_arrow(res.data.logs))

                    if logs_df.height > 0:
                        data.append(logs_df.get_column("address").unique())

                    logger.info(f"processed up to block {res.next_block}")

                if len(data) == 0:
                    continue

                data_concat: pl.Series = pl.concat(data)

                addr_series_list.append(data_concat.unique())

                logger.info(f"addr_series_list: {addr_series_list}")

                return addr_series_list
            
            except Exception as e:
                logger.error(f"Error processing signature {sig}: {e}")
                logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                return []
    
    async def get_parquet(self, params: GetParquetParams):
        try:

            topic0 = hypersync.signature_to_topic0(params.signature)
            query = Query(
                from_block=params.from_block,
                to_block=None if params.to_block is None else params.to_block + 1,
            logs=[LogSelection(
                topics = [[
                    topic0
                ]]
            )],
            field_selection=FieldSelection(
                log=[e.value for e in LogField],
                block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                # block=[e.value for e in BlockField],
                # transaction=[e.value for e in TransactionField],
            ),
        )
        
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

            while True:
                res = await receiver.recv()

                if res is None:
                    break

                data.append_data(res)
                
                if data.num_logs > params.items_per_section:
                    data.write_parquet()

                logger.info(f"processed up to block {res.next_block}, num_logs={data.num_logs}")

                logger.info(f"Data: {data}")
                
        except Exception as e:
            logger.error(f"Error while processing parquet: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")

        if data.num_logs > 0:
            data.write_parquet()
    

    async def get_data(self, from_block: int, to_block: int) -> Data:
        try:
            events_data = {}
            logger.info(f"Events: {[event.column_mapping for event in self.config.events]}")

            contract_addr_list = await self.get_contract_addr_list(self.config.contract_identifier_signatures)

            logger.info(f"contract_addr_list: {contract_addr_list}")

            for event in self.config.events:
                sig = event.signature
                name = event.name
                logger.info(f"Processing {name}\nsignature is {sig}\ntopic0 is {hypersync.signature_to_topic0(sig)}")

                await self.get_parquet(GetParquetParams(
                    client=self.client,
                    column_mapping=event.column_mapping,
                    event_name=name,
                    signature=sig,
                    contract_addr_list=contract_addr_list, 
                    from_block=self.config.from_block,
                    to_block=self.config.to_block,
                    items_per_section=self.config.items_per_section,
                ))

            logger.info(f"Processing signature {sig} with receiver {receiver}")

            event_logs = []
            
            while True:
                try:
                    logger.info("In try")
                    res = await receiver.recv()
                    logger.info(res)
                    if res is None:
                        break

                    if res.data.logs is not None:
                        logs_df = pl.from_arrow(res.data.logs)
                        decoded_logs_df = pl.from_arrow(res.data.decoded_logs)
                        blocks_df = pl.from_arrow(res.data.blocks)

                        decoded_logs_df = decoded_logs_df.rename(lambda n: f"decoded_{n}")
                        blocks_df = blocks_df.rename(lambda n: f"block_{n}")

                        combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")
                        
                        if combined_df.height > 0:
                            event_logs.append(combined_df)

                    logger.debug(f"Processed up to block {res.next_block}")

                except Exception as e:
                    logger.error(f"Error processing block data: {e}")
                    continue

            if event_logs:
                events_data[event.name] = pl.concat(event_logs)
            else:
                events_data[event.name] = pl.DataFrame()

        except Exception as e:
            logger.error(f"Error processing event {event.name}: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            events_data[event.name] = pl.DataFrame()

        return Data(
            blocks=None,
            transactions=None,
            events=events_data
        )

        """except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise """