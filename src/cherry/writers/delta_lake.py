import logging
from typing import Dict, Optional

import pyarrow as pa
from deltalake import DataCatalog, DeltaTable, write_deltalake

from ..config import DeltaLakeWriterConfig
from ..writers.base import DataWriter

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: DeltaLakeWriterConfig):
        logger.debug("Initializing DeltaLake writer...")
        self.config = config
        self.storage_options = config.storage_options or {}

    def _get_table_path(self, table_name: str) -> str:
        """Get the Delta Lake table path using file path approach."""
        return f"{self.config.table_uri}/{table_name}"

    def _resolve_catalog(self) -> Optional[DataCatalog]:
        """Resolve string catalog names to DataCatalog enum values."""
        if self.config.data_catalog is None:
            return None

        if isinstance(self.config.data_catalog, DataCatalog):
            return self.config.data_catalog

        # Handle string catalog names
        catalog_name = str(self.config.data_catalog).upper()

        # Try direct matching with known catalog types
        if catalog_name == "AWS":
            return DataCatalog.AWS
        elif catalog_name == "UNITY" or catalog_name == "UC":
            return DataCatalog.UNITY

        # Try to resolve from enum by name
        try:
            return getattr(DataCatalog, catalog_name)
        except (AttributeError, TypeError):
            raise ValueError(f"Unsupported data catalog: {self.config.data_catalog}")

    def _get_delta_table(self, table_name: str) -> DeltaTable:
        """Get a DeltaTable instance using either file path or catalog."""
        # Path-based approach (takes precedence)
        if self.config.table_uri:
            table_path = self._get_table_path(table_name)
            return DeltaTable(table_path, storage_options=self.storage_options)

        # Catalog-based approach
        catalog = self._resolve_catalog()

        # Unity Catalog uses special URL format
        if catalog == DataCatalog.UNITY:
            catalog_name = self.config.catalog_options.get("catalog_name", "main")
            uc_url = f"uc://{catalog_name}.{self.config.database_name}.{table_name}"
            return DeltaTable(uc_url, storage_options=self.storage_options)

        # Other catalogs use from_data_catalog
        return DeltaTable.from_data_catalog(
            data_catalog=catalog,
            database_name=self.config.database_name,
            table_name=table_name,
            storage_options=self.storage_options,
        )

    def _get_table_uri(self, table_name: str) -> str:
        """Get the Delta Lake table URI for writing."""
        # Path-based approach (takes precedence)
        if self.config.table_uri:
            return self._get_table_path(table_name)

        # Unity Catalog uses special URL format
        catalog = self._resolve_catalog()
        if catalog == DataCatalog.UNITY:
            catalog_name = self.config.catalog_options.get("catalog_name", "main")
            return f"uc://{catalog_name}.{self.config.database_name}.{table_name}"

        # For other catalogs, get the URI from the DeltaTable object
        delta_table = self._get_delta_table(table_name)
        return delta_table.table_uri

    async def write_table(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        """Write data to a Delta Lake table, creating it if it doesn't exist."""
        logger.debug(f"Writing table: {table_name}")
        table_uri = self._get_table_uri(table_name)

        # The 'append' mode will create the table if it doesn't exist
        write_deltalake(
            table_uri,
            record_batch,
            mode="append",
            storage_options=self.storage_options,
        )

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push multiple tables to Delta Lake, creating them if they don't exist."""
        for table_name, table_data in data.items():
            await self.write_table(table_name, table_data)
