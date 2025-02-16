import dlt
from dlt.sources.helpers import requests
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime
import psycopg2
import sys

# PostgreSQL connection settings (for testing)
PG_SETTINGS = {
    'database': 'postgres',
    'schema': 'public',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

@dlt.source(max_table_nesting=0)
def chess_resource(player_data):
    print("Creating resource...", flush=True)
    return dlt.resource(
        name='player',
        write_disposition='replace',
        primary_key=['username'],
        table_name='player',
        standalone=True,
        parallelized=True
    )(lambda: [player_data])

def run_pipeline(pipeline_name: str, dataset_name: str, data, name: str):
    start_time = time.time()
    print(f"\nStarting {name} at {datetime.now()}", flush=True)
    
    try:
        print(f"{name}: Creating pipeline...", flush=True)
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination='postgres',
            dataset_name=dataset_name
        )
        print(f"{name}: Pipeline created", flush=True)

        print(f"{name}: Starting extraction...", flush=True)
        resource = chess_resource(data)
        print(f"{name}: Resource created", flush=True)
        
        print(f"{name}: Running pipeline...", flush=True)
        info = pipeline.run(resource)
        print(f"{name}: Pipeline completed with info: {info}", flush=True)
        
        end_time = time.time()
        print(f"Completed {name} at {datetime.now()}", flush=True)
        print(f"Time taken for {name}: {end_time - start_time:.2f} seconds", flush=True)
        
        # Verify row count
        print(f"{name}: Verifying row count...", flush=True)
        conn = psycopg2.connect(**PG_SETTINGS)
        cur = conn.cursor()
        cur.execute(f"SELECT count(*) FROM {dataset_name}.player")
        count = cur.fetchone()[0]
        print(f"{name}: Loaded {count} rows to {dataset_name}", flush=True)
        cur.close()
        conn.close()
        print(f"{name}: Verification complete", flush=True)

    except Exception as e:
        print(f"Error in {name}: {str(e)}", flush=True)
        raise

if __name__ == "__main__":
    try:
        # List of players to fetch data for
        players = ['magnuscarlsen', 'rpragchess']
        
        # Fetch data
        print("Fetching player data...", flush=True)
        results = [requests.get(f'https://api.chess.com/pub/player/{player}').json() 
                  for player in players]
        print(f"Fetched data for {len(results)} players", flush=True)

        # Run pipelines in parallel
        print("\nStarting parallel pipeline execution...", flush=True)
        with ThreadPoolExecutor(max_workers=2) as executor:
            print("Submitting Pipeline 1...", flush=True)
            future1 = executor.submit(
                run_pipeline, 
                'chess_pipeline_1', 
                'player_data_1', 
                results, 
                "Pipeline 1"
            )
            print("Submitting Pipeline 2...", flush=True)
            future2 = executor.submit(
                run_pipeline, 
                'chess_pipeline_2', 
                'player_data_2', 
                results, 
                "Pipeline 2"
            )
            
            print("Waiting for pipelines to complete...", flush=True)
            print("Waiting for Pipeline 1...", flush=True)
            result1 = future1.result(timeout=60)
            print("Pipeline 1 completed", flush=True)
            
            print("Waiting for Pipeline 2...", flush=True)
            result2 = future2.result(timeout=60)
            print("Pipeline 2 completed", flush=True)

        print("\nAll pipelines completed successfully", flush=True)

    except Exception as e:
        print(f"\nFatal error: {str(e)}", flush=True)
        raise