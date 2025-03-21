from cherry import config as cc
from cherry.config import (
    Base58EncodeConfig,
    StepKind,
)
from cherry import run_pipelines, Context
from typing import Dict
from cherry_core import ingest
import logging
import os
import asyncio
from dotenv import load_dotenv
import traceback
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
import pyarrow as pa

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())
logger = logging.getLogger(__name__)

default_start_block = 317617480


async def get_start_block(client: AsyncClient) -> int:
    try:
        res = await client.query("SELECT MAX(block_slot) FROM token_balances")
        return res.result_rows[0][0] or default_start_block
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return default_start_block


async def join_data(data: Dict[str, pa.Table], _: cc.Step) -> Dict[str, pa.Table]:
    blocks = data["blocks"]
    data["transactions"]
    token_balances = data["token_balances"]

    blocks = blocks.rename_columns(["block_slot", "block_timestamp"])

    token_balances = token_balances.join(blocks, keys="block_slot")
    # token_balances = token_balances.join(transactions, keys=["block_slot", "transaction_index"])

    return {"token_balances": token_balances}


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
                    token_balances=[
                        ingest.svm.TokenBalanceRequest(
                            pre_mint=["27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"],
                            include_transactions=True,
                        ),
                    ],
                    fields=ingest.svm.Fields(
                        block=ingest.svm.BlockFields(
                            slot=True,
                            timestamp=True,
                        ),
                        transaction=ingest.svm.TransactionFields(
                            block_slot=True,
                            transaction_index=True,
                            signatures=True,
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
                "token_balances": ["block_slot"],
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
                        name="join_data",
                        kind="join_data",
                    ),
                    cc.Step(
                        name="base58_encode",
                        kind=StepKind.BASE58_ENCODE,
                        config=Base58EncodeConfig(),
                    ),
                ],
            )
        },
    )

    context = Context()

    context.add_step("join_data", join_data)

    await run_pipelines(config, context)


if __name__ == "__main__":
    asyncio.run(main())
