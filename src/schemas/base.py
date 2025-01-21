from typing import Dict
import pyarrow as pa
import polars as pl

class SchemaConverter:
    """Converts Arrow schemas to different formats"""
    
    @staticmethod
    def to_polars(arrow_schema: pa.Schema) -> Dict[str, pl.DataType]:
        """Convert Arrow schema to Polars schema"""
        type_map = {
            pa.string(): pl.Utf8,
            pa.int64(): pl.Int64,
            pa.bool_(): pl.Boolean,
            pa.float64(): pl.Float64,
            pa.binary(): pl.Binary,
            pa.timestamp('ns'): pl.Datetime,
            pa.date32(): pl.Date,
            pa.decimal128(38, 18): pl.Decimal(38, 18)
        }
        
        return {
            field.name: type_map.get(field.type, pl.Utf8)
            for field in arrow_schema
        }
    
    @staticmethod
    def to_sql(arrow_schema: pa.Schema, table_name: str) -> str:
        """Convert Arrow schema to SQL CREATE TABLE statement"""
        type_map = {
            pa.string(): "TEXT",
            pa.int64(): "BIGINT",
            pa.bool_(): "BOOLEAN",
            pa.float64(): "DOUBLE PRECISION",
            pa.binary(): "BYTEA",
            pa.timestamp('ns'): "TIMESTAMP",
            pa.date32(): "DATE",
            pa.decimal128(38, 18): "DECIMAL(38,18)"
        }
        
        fields = [
            f"{field.name} {type_map.get(field.type, 'TEXT')}"
            for field in arrow_schema
        ]
        
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)})"