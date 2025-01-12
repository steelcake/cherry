# SQL schemas for database tables
BLOCKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS blocks (
                    number BIGINT,
                    timestamp BIGINT
)
"""

TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
                    transaction_hash VARCHAR(66),
                    block_number BIGINT,
                    from_address VARCHAR(42),
                    to_address VARCHAR(42),
                    value NUMERIC(78,0)
)
"""

EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
                    id SERIAL,
                    transaction_hash VARCHAR(66),
                    block_number BIGINT,
                    from_address VARCHAR(42),
                    to_address VARCHAR(42),
                    value NUMERIC(78,0),
                    event_name VARCHAR(100),
                    contract_address VARCHAR(42),
                    topic0 VARCHAR(66),
                    raw_data TEXT
)
""" 