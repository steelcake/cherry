from cherry import config as cc
from cherry.config import (
    Base58EncodeConfig,
    ClickHouseSkipIndex,
    StepKind,
)
from cherry import run_pipelines, Context
from cherry_core import ingest, base58_encode_bytes
import logging
import os
import asyncio
from dotenv import load_dotenv
import traceback
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from typing import Dict
import pyarrow as pa
import datafusion as df

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)

default_start_block = 269828500


async def get_start_block(client: AsyncClient) -> int:
    try:
        res = await client.query("SELECT MAX(block_slot) FROM swaps")
        return res.result_rows[0][0] or default_start_block
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return default_start_block

async def prepare_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    ctx = df.SessionContext()

    blocks = ctx.create_dataframe([data["blocks"].to_batches()])
    instructions = ctx.create_dataframe([data["instructions"].to_batches()])
    token_balances = ctx.create_dataframe([data["token_balances"].to_batches()])

    blocks = blocks.with_column_renamed("slot", "block_slot").with_column_renamed("timestamp", "block_timestamp")

    swaps = instructions.join(token_balances, left_on=[
        "block_slot", "transaction_index", "a0", "a4"
    ], right_on=[
        "block_slot", "transaction_index", "pre_owner", "account"
    ])
    
    swaps.union()

    return { "swaps": swaps } 

async def main():
    clickhouse_client = await clickhouse_connect.get_async_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "blockchain"),
    )

    from_block = await get_start_block(clickhouse_client)
    logger.info(f"starting to ingest from block {from_block}")

    provider = cc.Provider(
        name="sqd_portal",
        config=ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url="https://portal.sqd.dev/datasets/solana-mainnet",
            query=ingest.Query(
                kind=ingest.QueryKind.SVM,
                params=ingest.svm.Query(
                    from_block=from_block,
                    instructions=[
                        ingest.svm.InstructionRequest(
                            program_id=["CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"],
                            d8=[
                                str(
                                    base58_encode_bytes(
                                        bytes.fromhex("8fbe5adac41e33de")
                                    )
                                )
                            ],
                            is_committed=True,
                            # inner_instructions=True,
                            transaction_token_balances=True,
                        ),
                    ],
                    fields=ingest.svm.Fields(
                        block=ingest.svm.BlockFields(
                            slot=True,
                            timestamp=True,
                        ),
                        instruction=ingest.svm.InstructionFields(
                            block_slot=True,
                            transaction_index=True,
                            a0=True,
                            a4=True,
                            a5=True,
                        ),
                        token_balance=ingest.svm.TokenBalanceFields(
                            block_slot=True,
                            transaction_index=True,
                            pre_amount=True,
                            post_amount=True,
                            pre_owner=True,
                            post_owner=True,
                            pre_mint=True,
                            post_mint=True,
                            account=True,
                        ),
                    ),
                ),
            ),
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=clickhouse_client,
            order_by={
                "swaps": ["block_slot"],
            },
            skip_index={
                "swaps": [
                    ClickHouseSkipIndex(
                        name="swap_timestamp_idx",
                        val="block_timestamp",
                        type_="minmax",
                        granularity=1,
                    ),
                    ClickHouseSkipIndex(
                        name="swap_owner_idx",
                        val="owner",
                        type_="bloom_filter(0.01)",
                        granularity=1,
                    ),
                ]
            },
        ),
    )

    config = cc.Config(
        project_name="my_project",
        description="My description",
        pipelines={
            "my_pipeline": cc.Pipeline(
                provider=provider,
                writer=writer,
                steps=[
                    cc.Step(
                        name="base58_encode",
                        kind=StepKind.BASE58_ENCODE,
                        config=Base58EncodeConfig(),
                    ),
                    cc.Step(
                        name="prepare_data",
                        kind="prepare_data",
                    ),
                ],
            )
        },
    )

    context = Context()

    context.add_step("prepare_data", prepare_data)

    await run_pipelines(config, context)


if __name__ == "__main__":
    asyncio.run(main())
