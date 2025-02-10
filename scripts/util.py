from typing import List
import pandas as pd
import polars

def prepare_and_insert_df(
    df_list: List[polars.DataFrame],
    table_name: str,
    database: str,
    s3_path: str,
):
    if len(df_list) == 0:
        print(f"skipping writing empty parquet for {table_name}")
        return
    
    polars_df = polars.concat(df_list)

    for (i, col) in enumerate(polars_df.iter_columns()):
        if col.dtype == polars.UInt64:
            polars_df.replace_column(i, col.cast(polars.Int64))
        elif col.dtype == polars.UInt32:
            polars.df.replace_column(i, col.cast(polars.Int64))
        elif col.dtype == polars.UInt16:
            polars.df.replace_column(i, col.cast(polars.Int32))
        elif col.dtype == polars.UInt8:
            polars.df.replace_column(i, col.cast(polars.Int16))

    dataframe = polars_df.to_pandas()
    dataframe["block_date"] = pd.to_datetime(dataframe["block_timestamp"])

    write_parquet_table(
        dataframe,
        table_name,
        database,
        s3_path,
    )
