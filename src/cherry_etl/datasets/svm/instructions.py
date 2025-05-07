from typing import Dict, Optional, Any
from cherry_core import ingest
from cherry_core.svm_decode import InstructionSignature
from cherry_etl import config as cc
import polars as pl
import logging

logger = logging.getLogger(__name__)


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


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    program_id: str,
    instruction_signature: InstructionSignature,
    from_block: int = 0,
    to_block: Optional[int] = None,
    dataset_name: Optional[str] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    if dataset_name is None:
        decode_instructions_config = cc.SvmDecodeInstructionsConfig(
            instruction_signature=instruction_signature,
            hstack=True,
            allow_decode_fail=True,
        )
    else:
        decode_instructions_config = cc.SvmDecodeInstructionsConfig(
            instruction_signature=instruction_signature,
            hstack=True,
            allow_decode_fail=True,
            output_table=f"{dataset_name}_decoded_instructions",
        )

    query = ingest.Query(
        kind=ingest.QueryKind.SVM,
        params=ingest.svm.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=True,
            fields=ingest.svm.Fields(
                instruction=ingest.svm.InstructionFields(
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    instruction_address=True,
                    program_id=True,
                    a0=True,
                    a1=True,
                    a2=True,
                    a3=True,
                    a4=True,
                    a5=True,
                    a6=True,
                    a7=True,
                    a8=True,
                    a9=True,
                    rest_of_accounts=True,
                    data=True,
                    error=True,
                    is_committed=True,
                    has_dropped_log_messages=True,
                ),
                block=ingest.svm.BlockFields(
                    hash=True,
                    timestamp=True,
                ),
                transaction=ingest.svm.TransactionFields(
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    signature=True,
                ),
            ),
            instructions=[
                ingest.svm.InstructionRequest(
                    program_id=[program_id],
                    discriminator=[instruction_signature.discriminator],
                    include_transactions=True,
                )
            ],
        ),
    )

    steps = [
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=process_data,
                context={"discriminator": instruction_signature.discriminator},
            ),
        ),
        cc.Step(
            kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS, config=decode_instructions_config
        ),
        cc.Step(
            kind=cc.StepKind.JOIN_SVM_TRANSACTION_DATA,
            config=cc.JoinSvmTransactionDataConfig(),
        ),
        cc.Step(
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(
                join_left_on=["block_hash"], join_blocks_on=["hash"]
            ),
        ),
        cc.Step(
            kind=cc.StepKind.BASE58_ENCODE,
            config=cc.Base58EncodeConfig(),
        ),
    ]

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )
