import dlt
from dlt.sources.helpers import requests
from concurrent.futures import ThreadPoolExecutor
import duckdb
import time
from datetime import datetime

# Create the first dlt pipeline
pipeline_1 = dlt.pipeline(
    pipeline_name='chess_pipeline_1',
    destination='duckdb',
    dataset_name='player_data',
    dev_mode=True
)

# Create the second dlt pipeline
pipeline_2 = dlt.pipeline(
    pipeline_name='chess_pipeline_2',
    destination='duckdb',
    dataset_name='player_data',
    dev_mode=True
)

# Function to fetch data for a player
def fetch_player_data(player):
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    return response.json()

# Function to read and print data from DuckDB
def print_duckdb_data(db_path: str):
    print(f"\nReading data from {db_path}")
    try:
        # Connect to DuckDB database
        conn = duckdb.connect(db_path)

        # Print all available schemas
        print("\nAvailable schemas:")
        schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        for schema in schemas:
            print(f"- {schema[0]}")

        # Query the data directly from the player_data schema
        query = "SELECT * FROM player_data.player"
        data = conn.execute(query).fetchall()
        
        # Get column names
        columns = conn.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'player_data' 
            AND table_name = 'player'
        """).fetchall()
        
        # Print results
        print(f"\nColumns: {[col[0] for col in columns]}")
        print("Data:")
        for row in data:
            print(row)

        conn.close()
    except Exception as e:
        print(f"Error reading from {db_path}: {e}")

def run_pipeline(pipeline, data, name):
    start_time = time.time()
    print(f"\nStarting {name} at {datetime.now()}")
    try:
        pipeline.run(data, table_name='player', write_disposition='replace')
        end_time = time.time()
        print(f"Completed {name} at {datetime.now()}")
        print(f"Time taken for {name}: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"Error in {name}: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # List of players to fetch data for
        players = ['magnuscarlsen', 'rpragchess']
        
        # Fetch data sequentially
        print("Fetching player data...")
        results = [fetch_player_data(player) for player in players]
        print(f"Fetched data for {len(results)} players")

        # Run pipelines in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            print("\nStarting parallel pipeline execution...")
            future1 = executor.submit(run_pipeline, pipeline_1, results, "Pipeline 1")
            future2 = executor.submit(run_pipeline, pipeline_2, results, "Pipeline 2")
            
            # Wait for both pipelines to complete with timeout
            future1.result(timeout=60)  # 60 seconds timeout
            future2.result(timeout=60)  # 60 seconds timeout

        print("\nData processing completed successfully")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        raise
    # Print data from both databases/Issue with this creating arbitrary schemas
    #print_duckdb_data("chess_pipeline_1.duckdb")
    #print_duckdb_data("chess_pipeline_2.duckdb")
