from ..writers.base import DataWriter
from ..config.parser import Writer, WriterKind
from ..writers.local_parquet import ParquetWriter
from ..writers.aws_wrangler_s3 import AWSWranglerWriter
import logging

logger = logging.getLogger(__name__)

def create_writer(writer: Writer) -> DataWriter:
    match writer.kind:
        case WriterKind.LOCAL_PARQUET:
            return ParquetWriter(writer.config)
        case WriterKind.AWS_WRANGLER_S3:
            return AWSWranglerWriter(writer.config)
        case _:
            raise ValueError(f"Invalid writer kind: {writer.kind}")