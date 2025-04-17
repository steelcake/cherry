from typing import Dict, Optional, Union
from cherry_core import ingest
from cherry_core.svm_decode import InstructionSignature
from cherry_etl import config as cc
import polars as pl
import logging

logger = logging.getLogger(__name__)


def process_data(
    data: Dict[str, pl.DataFrame], discriminator: Union[bytes, None]
) -> Dict[str, pl.DataFrame]:
    df = data["instructions"]

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
        )
    elif len(discriminator) == 2:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d2=[discriminator],
        )
    elif len(discriminator) == 4:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d4=[discriminator],
        )
    elif len(discriminator) == 8:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d8=[discriminator],
        )
    elif len(discriminator) > 8:
        instruction_request = ingest.svm.InstructionRequest(
            program_id=[program_id],
            d8=[discriminator[:8]],
        )
    else:
        raise Exception(f"Unsupported discriminator length: {len(discriminator)}")

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
            ),
            instructions=[instruction_request],
        ),
    )

    return cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                    context=discriminator,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=instruction_signature,
                    hstack=True,
                    allow_decode_fail=True,
                ),
            ),
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
            cc.Step(kind=cc.StepKind.BASE58_ENCODE, config=cc.Base58EncodeConfig()),
        ],
    )
