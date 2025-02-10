import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime
import json
import random

# PostgreSQL connection settings
PG_SETTINGS = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

def create_table(table_name):
    """Create a table if it doesn't exist"""
    print(f"Creating table {table_name}...", flush=True)
    conn = psycopg2.connect(**PG_SETTINGS)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255),
                    data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Clear existing data
            cur.execute(f"TRUNCATE TABLE {table_name}")
            print(f"Table {table_name} created and cleared successfully", flush=True)
    finally:
        conn.close()

def write_data(table_name, data, name):
    """Write data to specified table with random delays to simulate work"""
    start_time = time.time()
    print(f"\nStarting {name} write at {datetime.now()}", flush=True)
    
    conn = psycopg2.connect(**PG_SETTINGS)
    try:
        with conn.cursor() as cur:
            for i, item in enumerate(data):
                # Add random delay every few records to simulate work
                if i % 10 == 0:
                    delay = random.uniform(0.1, 0.5)
                    print(f"{name}: Processing batch {i}, sleeping for {delay:.2f}s", flush=True)
                    time.sleep(delay)
                
                cur.execute(
                    f"INSERT INTO {table_name} (username, data) VALUES (%s, %s)",
                    (item['username'], json.dumps(item))
                )
                
                # Commit every 10 records
                if i % 10 == 9:
                    conn.commit()
                    print(f"{name}: Committed batch {i+1}", flush=True)
            
            # Final commit
            conn.commit()
            
            # Verify count
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            
        end_time = time.time()
        print(f"Completed {name} at {datetime.now()}", flush=True)
        print(f"Time taken for {name}: {end_time - start_time:.2f} seconds", flush=True)
        print(f"Wrote {count} rows to {table_name}", flush=True)
        
    except Exception as e:
        print(f"Error in {name}: {str(e)}", flush=True)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        # Generate larger sample data
        sample_data = []
        for i in range(100):  # 100 records
            sample_data.append({
                'username': f'user{i}',
                'rating': random.randint(1500, 3000),
                'title': random.choice(['GM', 'IM', 'FM', 'CM']),
                'country': random.choice(['Norway', 'USA', 'Russia', 'China', 'India']),
                'games_played': random.randint(100, 1000),
                'win_rate': random.uniform(0.4, 0.7)
            })

        # Create tables
        create_table('concurrent_test_1')
        create_table('concurrent_test_2')

        # Run concurrent writes
        print("\nStarting concurrent writes...", flush=True)
        with ThreadPoolExecutor(max_workers=2) as executor:
            print("Submitting both writes simultaneously...", flush=True)
            future1 = executor.submit(
                write_data,
                'concurrent_test_1',
                sample_data,
                "Write 1"
            )
            future2 = executor.submit(
                write_data,
                'concurrent_test_2',
                sample_data,
                "Write 2"
            )
            
            print("Both writes are now running concurrently...", flush=True)
            future1.result(timeout=60)
            future2.result(timeout=60)

        print("\nAll writes completed successfully", flush=True)

    except Exception as e:
        print(f"\nFatal error: {str(e)}", flush=True)
        raise 