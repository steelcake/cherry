from typing import Any, Dict, Optional
from cherry_core import ingest
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType, FixedArray
from cherry_etl import config as cc
import polars as pl
import logging
import base58

logger = logging.getLogger(__name__)


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    df = data["instructions"]

    processed_df = df.filter(
        pl.col("data").bin.starts_with(base58.b58decode("VBuTFX8Ey5wmpzJ9WerNBK"))
    )
    data["instructions"] = processed_df

    return data


def make_pipeline(
    provider: ingest.ProviderConfig,
    writer: cc.Writer,
    from_block: int = 0,
    to_block: Optional[int] = None,
) -> cc.Pipeline:
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

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
            instructions=[ingest.svm.InstructionRequest(
                program_id=["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"],
                d8=["fBXSauZxba4"]
            )],
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
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=InstructionSignature(
                        discriminator="VBuTFX8Ey5wmpzJ9WerNBK",
                        params=[
                            ParamInput(
                                name="Amm",
                                param_type=FixedArray(DynType.U8, 32),
                            ),
                            ParamInput(
                                name="InputMint",
                                param_type=FixedArray(DynType.U8, 32),
                            ),
                            ParamInput(
                                name="InputAmount",
                                param_type=DynType.U64,
                            ),
                            ParamInput(
                                name="OutputMint",
                                param_type=FixedArray(DynType.U8, 32),
                            ),
                            ParamInput(
                                name="OutputAmount",
                                param_type=DynType.U64,
                            ),
                        ],
                        accounts_names=[],
                    ),
                    hstack=True,
                    allow_decode_fail=True,
                ),
            ),
            cc.Step(kind=cc.StepKind.JOIN_BLOCK_DATA, config=cc.JoinBlockDataConfig()),
            cc.Step(kind=cc.StepKind.BASE58_ENCODE, config=cc.Base58EncodeConfig()),
        ],
    )
