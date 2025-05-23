from cherry_etl import steps as cs
from cherry_etl import config as cc
from cherry_etl import utils
import pyarrow as pa
import base58
import binascii
import datafusion
import polars as pl
from typing import Optional, Any


def test_base58_encode():
    numbers = pa.array([1, 2], type=pa.uint8())
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays([numbers, names], names=["numbers", "names"])

    data = {"data": table}

    data = cs.base58_encode.execute(data, cc.Base58EncodeConfig())

    data = data["data"]

    assert data.column("numbers").combine_chunks() == numbers

    for expected, name in zip(names, data.column("names").combine_chunks()):
        assert base58.b58encode(expected.as_py()).decode("utf-8") == str(name)


def test_cast():
    my_list = [1, 2]
    numbers = pa.array(my_list, type=pa.uint8())

    table = pa.Table.from_arrays([numbers], names=["numbers"])

    data = {"data": table}

    data = cs.cast.execute(
        data,
        cc.CastConfig(
            table_name="data",
            mappings={
                "numbers": pa.int64(),
            },
        ),
    )

    data = data["data"]

    numbers = pa.array(my_list, type=pa.int64())

    assert data.column("numbers").combine_chunks() == numbers


def test_cast_by_type():
    my_list = [1, 2]
    numbers = pa.array(my_list, type=pa.uint8())
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays([numbers, names], names=["numbers", "names"])

    data = {"data": table}

    data = cs.cast_by_type.execute(
        data,
        cc.CastByTypeConfig(
            from_type=pa.uint8(),
            to_type=pa.int16(),
        ),
    )

    data = data["data"]

    numbers = pa.array(my_list, type=pa.int16())

    assert data.column("numbers").combine_chunks() == numbers
    assert data.column("names").combine_chunks() == names


def test_evm_decode_events():
    return


def test_evm_validate_block_data():
    return


def test_hex_encode():
    numbers = pa.array([1, 2], type=pa.uint8())
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays([numbers, names], names=["numbers", "names"])

    data = {"data": table}

    data = cs.hex_encode.execute(
        data,
        cc.HexEncodeConfig(
            prefixed=False,
        ),
    )

    data = data["data"]

    assert data.column("numbers").combine_chunks() == numbers

    for expected, name in zip(names, data.column("names").combine_chunks()):
        assert binascii.hexlify(expected.as_py()).decode("utf-8") == str(name)


def test_u256_to_binary():
    numbers = pa.array([1, 2], type=pa.decimal256(76, 0))
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays([numbers, names], names=["numbers", "names"])

    data = {"data": table}

    data = cs.u256_to_binary.execute(
        data,
        cc.U256ToBinaryConfig(),
    )

    data = data["data"]

    assert data.column("names").combine_chunks() == names

    assert data.column("numbers").type == pa.binary()


def test_set_chain_id():
    numbers = pa.array([1, 2], type=pa.decimal256(76, 0))
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays(
        [numbers, names, numbers], names=["numbers", "names", "chain_id"]
    )
    table2 = pa.Table.from_arrays(
        [numbers, names, names], names=["numbers", "names", "other_names"]
    )

    data = {"table": table, "table2": table2}

    data = cs.set_chain_id.execute(data, cc.SetChainIdConfig(chain_id=69))

    table = data["table"]
    table2 = data["table2"]

    assert table.column("names").combine_chunks() == names
    assert table2.column("names").combine_chunks() == names
    assert table2.column("other_names").combine_chunks() == names

    assert table.column("chain_id").combine_chunks() == pa.repeat(
        pa.scalar(69, type=pa.uint64()), 2
    )
    assert table2.column("chain_id").combine_chunks() == pa.repeat(
        pa.scalar(69, type=pa.uint64()), 2
    )


def test_svm_anchor_discriminator():
    assert utils.svm_anchor_discriminator("swap_v2").hex() == "2b04ed0b1ac91e62"
    assert utils.svm_anchor_discriminator("swap").hex() == "f8c69e91e17587c8"


def datafusion_step_runner(
    session_ctx: datafusion.SessionContext,
    data: dict[str, datafusion.DataFrame],
    ctx: Optional[Any],
) -> dict[str, datafusion.DataFrame]:
    _ = session_ctx
    _ = ctx

    out = data["table"].select((datafusion.col("numbers") + 2).alias("numbers69"))

    return {"out": out}


def test_datafusion_step():
    numbers = pa.array([1, 2], type=pa.decimal256(76, 0))
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays(
        [numbers, names, numbers], names=["numbers", "names", "chain_id"]
    )
    table2 = pa.Table.from_arrays(
        [numbers, names, names], names=["numbers", "names", "other_names"]
    )

    data = {"table": table, "table2": table2}

    data = cs.datafusion_step.execute(
        data, cc.DataFusionStepConfig(runner=datafusion_step_runner)
    )

    assert data["out"].column("numbers69").combine_chunks() == pa.array(
        [3, 4], type=pa.decimal256(76, 0)
    )


def polars_step_runner(
    data: dict[str, pl.DataFrame], ctx: Optional[Any]
) -> dict[str, pl.DataFrame]:
    _ = ctx

    out = data["table"].select(pl.col("numbers").add(2).alias("numbers69"))

    return {"out": out}


def test_polars_step():
    numbers = pa.array([1, 2], type=pa.decimal128(38, 0))
    names = pa.array(["asd", "qwe"], type=pa.binary())

    table = pa.Table.from_arrays(
        [numbers, names, numbers], names=["numbers", "names", "chain_id"]
    )
    table2 = pa.Table.from_arrays(
        [numbers, names, names], names=["numbers", "names", "other_names"]
    )

    data = {"table": table, "table2": table2}

    data = cs.polars_step.execute(data, cc.PolarsStepConfig(runner=polars_step_runner))

    assert data["out"].column("numbers69").combine_chunks() == pa.array(
        [3, 4], type=pa.decimal128(38, 0)
    )
