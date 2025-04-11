from typing import Any, Dict, Optional
from cherry_core import ingest
from cherry_core.svm_decode import InstructionSignature, ParamInput, DynType
from cherry_etl import config as cc
import polars as pl
import logging
import base58

logger = logging.getLogger(__name__)


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    df = data["instructions"]

    processed_df = df.filter(
        pl.col("data").bin.starts_with(
            bytes(
                [228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226]
            )
        )
        & pl.col("program_id").bin.starts_with(
            bytes(
                [
                    4,
                    121,
                    213,
                    91,
                    242,
                    49,
                    192,
                    110,
                    238,
                    116,
                    197,
                    110,
                    206,
                    104,
                    21,
                    7,
                    253,
                    177,
                    178,
                    222,
                    163,
                    244,
                    142,
                    81,
                    2,
                    177,
                    205,
                    162,
                    86,
                    188,
                    19,
                    143,
                ]
            )
        )
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
            instructions=[ingest.svm.InstructionRequest()],
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
                        discriminator=base58.b58encode(
                            bytes(
                                [
                                    228,
                                    69,
                                    165,
                                    46,
                                    81,
                                    203,
                                    154,
                                    29,
                                    64,
                                    198,
                                    205,
                                    232,
                                    38,
                                    8,
                                    113,
                                    226,
                                ]
                            )
                        ).decode(),
                        params=[
                            ParamInput(
                                name="Amm",
                                param_type=DynType.FixedArray(DynType.U8, 32),
                            ),
                            ParamInput(
                                name="InputMint",
                                param_type=DynType.FixedArray(DynType.U8, 32),
                            ),
                            ParamInput(
                                name="InputAmount",
                                param_type=DynType.U64,
                            ),
                            ParamInput(
                                name="OutputMint",
                                param_type=DynType.FixedArray(DynType.U8, 32),
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
