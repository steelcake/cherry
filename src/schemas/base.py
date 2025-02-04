from typing import Dict
import pyarrow as pa
import polars as pl

class SchemaConverter:
    """Converts Arrow schemas to different formats"""
    
    # Define type mappings as class attributes for reuse and consistency
    ARROW_TO_POLARS = {
        pa.string(): pl.Utf8,
        pa.int64(): pl.Int64,
        pa.uint64(): pl.UInt64,  # Add explicit uint64 support
        pa.bool_(): pl.Boolean,
        pa.float64(): pl.Float64,
        pa.binary(): pl.Binary,
        pa.timestamp('ns'): pl.Datetime,
        pa.date32(): pl.Date,
        pa.decimal128(38, 18): pl.Decimal(38, 18)
    }
    
    ARROW_TO_SQL = {
        pa.string(): "TEXT",
        pa.int64(): "BIGINT",
        pa.uint64(): "BIGINT",  # Map uint64 to BIGINT
        pa.bool_(): "BOOLEAN",
        pa.float64(): "DOUBLE PRECISION",
        pa.binary(): "BYTEA",
        pa.timestamp('ns'): "TIMESTAMP",
        pa.date32(): "DATE",
        pa.decimal128(38, 18): "DECIMAL(38,18)"
    }
    
    @staticmethod
    def to_polars(arrow_schema: pa.Schema) -> Dict[str, pl.DataType]:
        """Convert Arrow schema to Polars schema"""
        
        return {
            field.name: 
            SchemaConverter.ARROW_TO_POLARS.get(field.type, pl.Utf8)
            for field in arrow_schema
        }    
    @staticmethod
    def to_sql(arrow_schema: pa.Schema, table_name: str) -> str:
        """Convert Arrow schema to SQL CREATE TABLE statement"""
        
        fields = [
            f"{field.name} {SchemaConverter.ARROW_TO_SQL.get(field.type, 'TEXT')}"
            for field in arrow_schema
        ]
        
        # Add primary key for blocks table
        if table_name == "blocks":
            fields.append("PRIMARY KEY (block_number)")
        # Add indexes for events table
        elif table_name == "events":
            fields.append("INDEX idx_events_block_number (block_number)")
            fields.append("INDEX idx_events_transaction_hash (transaction_hash)")
            fields.append("INDEX idx_events_address (address)")
        
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(fields)}
            );
        """
