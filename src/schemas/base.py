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
        arrow_fields = [
            (name, pa.string() if dtype == "string" else pa.int64())
            for name, dtype in self.fields.items()
        ]
        return pa.schema(arrow_fields)
        
    def to_polars(self) -> Dict[str, pl.DataType]:
        """Convert to Polars schema"""
        polars_types = {
            "string": pl.Utf8,
            "int64": pl.Int64,
            "bool": pl.Boolean,
            "float64": pl.Float64
        }
        return {
            name: polars_types[dtype]
            for name, dtype in self.fields.items()
        }
        
    def to_sql(self) -> str:
        """Convert to SQL CREATE TABLE statement"""
        sql_types = {
            "string": "TEXT",
            "int64": "BIGINT",
            "bool": "BOOLEAN",
            "float64": "FLOAT"
        }
        fields_sql = [
            f"{name} {sql_types[dtype]}"
            for name, dtype in self.fields.items()
        ]
        sql = f"CREATE TABLE IF NOT EXISTS {self.name} ("
        sql += ", ".join(fields_sql)
        sql += ")"
        return sql