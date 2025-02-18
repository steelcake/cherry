from ..writers.base import DataWriter
from ..config.parser import Writer, WriterKind
from ..writers.local_parquet import ParquetWriter

def create_writer(writer: Writer) -> DataWriter:
    match writer.kind:
        case WriterKind.LOCAL_PARQUET:
            return ParquetWriter(writer.config)
        case _:
            raise ValueError(f"Invalid writer kind: {writer.kind}")