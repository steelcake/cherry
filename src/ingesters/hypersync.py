import json, logging, aiohttp
import polars as pl
import pyarrow as pa
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.utils.logging_setup import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

class HypersyncIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config
        self.url = config.data_source[0].url
        self.events = {event.name: event for event in config.events}
        logger.info(f"Initialized HypersyncIngester with URL: {self.url}")

    async def get_data(self, from_block: int, to_block: int) -> Data:
        try:
            all_transactions = []
            blocks_data = {}
            
            # Sample data for logging
            sample_blocks = []
            sample_transactions = []
            sample_events = {name: [] for name in self.events.keys()}
            
            # Create Arrow schema for the stream
            schema = pa.schema([
                ("transaction_hash", pa.string()),
                ("block_number", pa.int64()),
                ("from_address", pa.string()),
                ("to_address", pa.string()),
                ("value", pa.int64()),
                ("event_name", pa.string()),
                ("contract_address", pa.string()),
                ("topic0", pa.string()),
                ("raw_data", pa.string())
            ])

            for event_name, event in self.events.items():
                query = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getLogs",
                    "params": [{
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "address": event.address[0] if event.address else None,
                        "topics": event.topics[0] if event.topics else None
                    }]
                }

                logger.info(f"Fetching {event_name} events from block {from_block} to {to_block}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.url, json=query) as response:
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}: {await response.text()}")

                        data = await response.json()
                        if "error" in data:
                            raise Exception(f"RPC Error: {data['error']}")

                        logs = data.get("result", [])
                        logger.info(f"Retrieved {len(logs)} {event_name} logs")

                        # Process logs and collect samples
                        for i, log in enumerate(logs):
                            block_num = int(log["blockNumber"], 16)
                            
                            # Store block data
                            block_data = {
                                "number": block_num,
                                "timestamp": int(log.get("timeStamp", "0"), 16) if "timeStamp" in log else 0
                            }
                            blocks_data[block_num] = block_data
                            
                            # Collect sample block data
                            if len(sample_blocks) < 2 and block_data not in sample_blocks:
                                sample_blocks.append(block_data)

                            # Convert log to transaction format
                            tx = {
                                "transaction_hash": log["transactionHash"],
                                "block_number": block_num,
                                "from_address": "0x" + log["topics"][1][-40:] if len(log["topics"]) > 1 else None,
                                "to_address": "0x" + log["topics"][2][-40:] if len(log["topics"]) > 2 else None,
                                "value": int(log["data"], 16),
                                "event_name": event_name,
                                "contract_address": log["address"],
                                "topic0": log["topics"][0],
                                "raw_data": log["data"]
                            }
                            all_transactions.append(tx)
                            
                            # Collect sample transaction data
                            if len(sample_transactions) < 2:
                                sample_transactions.append(tx)
                            
                            # Collect sample event data
                            if len(sample_events[event_name]) < 2:
                                sample_events[event_name].append({
                                    "block_number": block_num,
                                    "transaction_hash": log["transactionHash"],
                                    "event_name": event_name,
                                    "from": "0x" + log["topics"][1][-40:] if len(log["topics"]) > 1 else None,
                                    "to": "0x" + log["topics"][2][-40:] if len(log["topics"]) > 2 else None,
                                    "value": int(log["data"], 16)
                                })

            # Log sample data
            logger.info("Sample Data Examples:")
            logger.info("\nSample Blocks:")
            logger.info(json.dumps(sample_blocks, indent=2))
            
            logger.info("\nSample Transactions:")
            logger.info(json.dumps(sample_transactions, indent=2))
            
            logger.info("\nSample Events:")
            logger.info(json.dumps(sample_events, indent=2))

            # Convert blocks_data to DataFrame
            blocks_df = pl.DataFrame([
                {"number": block["number"], "timestamp": block["timestamp"]}
                for block in blocks_data.values()
            ]) if blocks_data else pl.DataFrame(schema={"number": pl.Int64, "timestamp": pl.Int64})

            # Process transactions through Arrow
            if all_transactions:
                arrow_table = pa.Table.from_pylist(all_transactions, schema=schema)
                
                # Create a buffer for the Arrow stream
                sink = pa.BufferOutputStream()
                with pa.ipc.RecordBatchStreamWriter(sink, arrow_table.schema) as writer:
                    writer.write_table(arrow_table)
                
                # Get the buffer and create a reader
                buf = sink.getvalue()
                reader = pa.ipc.RecordBatchStreamReader(pa.BufferReader(buf))

                # Process the Arrow stream in batches
                while True:
                    try:
                        batch = reader.read_next_batch()
                    except StopIteration:
                        break

            # Convert transactions to DataFrame
            transactions_df = pl.DataFrame(all_transactions) if all_transactions else pl.DataFrame(schema={
                "transaction_hash": pl.Utf8,
                "block_number": pl.Int64,
                "from_address": pl.Utf8,
                "to_address": pl.Utf8,
                "value": pl.Int64,
                "event_name": pl.Utf8,
                "contract_address": pl.Utf8,
                "topic0": pl.Utf8,
                "raw_data": pl.Utf8
            })

            # Log DataFrame statistics
            logger.info(f"\nDataFrame Statistics:")
            logger.info(f"Blocks DataFrame: {len(blocks_df)} rows")
            logger.info(f"Transactions DataFrame: {len(transactions_df)} rows")
            
            if len(blocks_df) > 0:
                logger.info("\nBlocks DataFrame Schema:")
                logger.info(blocks_df.schema)
                logger.info("\nSample Blocks DataFrame:")
                logger.info(blocks_df.head(2))
            
            if len(transactions_df) > 0:
                logger.info("\nTransactions DataFrame Schema:")
                logger.info(transactions_df.schema)
                logger.info("\nSample Transactions DataFrame:")
                logger.info(transactions_df.head(2))

            return Data(
                blocks=blocks_df,
                transactions=transactions_df,
                events={name: transactions_df.filter(pl.col("event_name") == name) 
                       for name in self.events.keys()}
            )

        except Exception as e:
            logger.error(f"Error fetching data from Hypersync: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 