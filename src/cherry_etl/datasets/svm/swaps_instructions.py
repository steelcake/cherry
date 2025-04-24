from typing import Dict, Optional, Any
from cherry_core import ingest
from cherry_core.svm_decode import InstructionSignature, LogSignature
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
    filter_logs = context.get("filter_logs", False)

    if discriminator is not None and len(discriminator) > 8:
        df = data["instructions"]
        processed_df = df.filter(pl.col("data").bin.starts_with(discriminator))
        data["instructions"] = processed_df

    if filter_logs:
        df = data["logs"]
        processed_df = df.filter(pl.col("kind") == "data")
        data["logs"] = processed_df

    return data


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    program_id: str,
    instruction_signature: InstructionSignature,
    from_block: int = 0,
    to_block: Optional[int] = None,
    dataset_name: Optional[str] = None,
    log_signature: Optional[LogSignature] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    discriminator = instruction_signature.discriminator
    if isinstance(discriminator, str):
        discriminator = bytes.fromhex(discriminator.strip("0x"))
    elif isinstance(discriminator, bytes):
        pass
    else:
        raise TypeError(
            f"discriminator must be bytes or str, got {type(discriminator)}"
        )

    if len(discriminator) == 1:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d1=[discriminator],
            include_transactions=True,
            include_logs=True,
        )
    elif len(discriminator) == 2:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d2=[discriminator],
            include_transactions=True,
            include_logs=True,
        )
    elif len(discriminator) == 4:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d4=[discriminator],
            include_transactions=True,
            include_logs=True,
        )
    elif len(discriminator) == 8:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d8=[discriminator],
            include_transactions=True,
            include_logs=True,
        )
    elif len(discriminator) > 8:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d8=[discriminator[:8]],
            include_transactions=True,
            include_logs=True,
        )
    else:
        raise Exception(f"Unsupported discriminator length: {len(discriminator)}")

    filter_logs = log_signature is not None

    if dataset_name is None:
        decode_instructions_config = cc.SvmDecodeInstructionsConfig(
            instruction_signature=instruction_signature,
            hstack=True,
            allow_decode_fail=True,
        )
        decode_logs_config = None
        if filter_logs:
            decode_logs_config = cc.SvmDecodeLogsConfig(
                log_signature=log_signature,
                allow_decode_fail=True,
                output_table=f"{dataset_name}_decoded_logs",
            )
    else:
        decode_instructions_config = cc.SvmDecodeInstructionsConfig(
            instruction_signature=instruction_signature,
            hstack=True,
            allow_decode_fail=True,
            output_table=f"{dataset_name}_decoded_instructions",
        )
        decode_logs_config = None
        if filter_logs:
            decode_logs_config = cc.SvmDecodeLogsConfig(
                log_signature=log_signature,
                allow_decode_fail=True,
                output_table=f"{dataset_name}_decoded_logs",
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
                log=ingest.svm.LogFields(
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    log_index=True,
                    instruction_address=True,
                    program_id=True,
                    kind=True,
                    message=True,
                ),
            ),
            instructions=[instruction_request],
        ),
    )

    steps = [
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=process_data,
                context={"discriminator": discriminator, "filter_logs": filter_logs},
            ),
        ),
        cc.Step(
            kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
            config=decode_instructions_config,
        ),
    ]

    if filter_logs and decode_logs_config is not None:
        steps.append(
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_LOGS,
                config=decode_logs_config,
            ),
        )

    steps.extend(
        [
            cc.Step(
                kind=cc.StepKind.JOIN_SVM_TRANSACTION_DATA,
                config=cc.JoinSvmTransactionDataConfig(),
            ),
            cc.Step(
                kind=cc.StepKind.JOIN_BLOCK_DATA,
                config=cc.JoinBlockDataConfig(),
            ),
            cc.Step(
                kind=cc.StepKind.BASE58_ENCODE,
                config=cc.Base58EncodeConfig(),
            ),
        ]
    )

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )
