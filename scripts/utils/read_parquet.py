import polars as pl
import os
import glob

pl.Config.set_tbl_cols(100)

# Get the current working directory
current_path = os.getcwd()

print(f"Current OS path: {current_path}")

"""# Check if any files matching the patterns exist
blocks_files = glob.glob("./data/blocks/*.parquet")
events_files = glob.glob("./data/events/approval/*.parquet")"""

# # Read all blocks Parquet files into a Polars DataFrame if they exist
# if blocks_files:
#     blocks_df = pl.read_parquet(blocks_files)  # Read all matching files
#     print("Blocks DataFrame:")
#     print(blocks_df)
# else:
#     print("No Blocks Parquet files found.")

# # Read all events Parquet files into a Polars DataFrame if they exist
# if events_files:
#     events_df = pl.read_parquet(events_files)  # Read all matching files
#     print("Events DataFrame:")
#     print(events_df)
# else:
#     print("No Events Parquet files found.")

blocks_df = pl.read_parquet("./data/blocks/blocks*.parquet")
print("Blocks DataFrame:", blocks_df.select([pl.col("number").min().alias("min_block_number")]))
