from enum import Enum
from typing import Dict, Any
import pyarrow as pa
import polars as pl

class SchemaType(Enum):
    ARROW = "arrow"
    POLARS = "polars"
    SQL = "sql"

class BlockchainSchema:
    """Base schema class for blockchain data"""
    
    def __init__(self, name: str, fields: Dict[str, Any]):
        self.name = name
        self.fields = fields
        
    def to_arrow(self) -> pa.Schema:
        """Convert to Arrow schema"""
        arrow_types = {
            "string": pa.string(),
            "int64": pa.int64(),
            "bool": pa.bool_(),
            "float64": pa.float64(),
            "binary": pa.binary(),
            "timestamp": pa.timestamp('ns'),
            "date": pa.date32(),
            "decimal": pa.decimal128(38, 18),  # Common for ETH amounts
            "list_string": pa.list_(pa.string()),
            "list_int": pa.list_(pa.int64()),
            "bytes32": pa.binary(32),  # For hashes
            "address": pa.string()  # ETH addresses
        }
        
        arrow_fields = [
            (name, arrow_types.get(dtype, pa.string()))  # Default to string if type not found
            for name, dtype in self.fields.items()
        ]
        return pa.schema(arrow_fields)
        
    def to_polars(self) -> Dict[str, pl.DataType]:
        """Convert to Polars schema"""
        polars_types = {
            "string": pl.Utf8,
            "int64": pl.Int64,
            "bool": pl.Boolean,
            "float64": pl.Float64,
            "binary": pl.Binary,
            "timestamp": pl.Datetime,
            "date": pl.Date,
            "decimal": pl.Decimal(38, 18),  # Common for ETH amounts
            "list_string": pl.List(pl.Utf8),
            "list_int": pl.List(pl.Int64),
            "bytes32": pl.Binary,  # For hashes
            "address": pl.Utf8  # ETH addresses
        }
        
        return {
            name: polars_types.get(dtype, pl.Utf8)  # Default to Utf8 if type not found
            for name, dtype in self.fields.items()
        }
        
    def to_sql(self) -> str:
        """Convert to SQL CREATE TABLE statement"""
        sql_types = {
            "string": "TEXT",
            "int64": "BIGINT",
            "bool": "BOOLEAN",
            "float64": "DOUBLE PRECISION",
            "binary": "BYTEA",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
            "decimal": "DECIMAL(38,18)",  # Common for ETH amounts
            "list_string": "TEXT[]",
            "list_int": "BIGINT[]",
            "bytes32": "BYTEA",  # For hashes
            "address": "CHAR(42)"  # ETH addresses (0x + 40 hex chars)
        }
        
        fields_sql = [
            f"{name} {sql_types.get(dtype, 'TEXT')}"  # Default to TEXT if type not found
            for name, dtype in self.fields.items()
        ]
        sql = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        sql += ", ".join(fields_sql)
        sql += ")"
        return sql