"""Writer utilities"""
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_output_path(base_path: str, table_name: str, start_block: int, end_block: int) -> str:
    """Generate output path with block range
    
    Args:
        base_path: Base path (e.g. 's3://bucket/folder' or 'data')
        table_name: Name of the table (e.g. 'approval_events')
        start_block: Starting block number
        end_block: Ending block number
        
    Returns:
        Full path including timestamp and block range
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Ensure base path doesn't end with slash
    base_path = base_path.rstrip('/')
    
    # Format with timestamp and block range
    filename = f"{timestamp}_{start_block:08d}_{end_block:08d}.parquet"
    
    # Combine paths
    full_path = f"{base_path}/{table_name}/{filename}"
    
    logger.debug(f"Generated output path: {full_path}")
    return full_path

def get_output_path_old(base_path: str, table_name: str, start_block: int, end_block: int) -> str:
    """Generate output path for parquet files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{start_block}_{end_block}.parquet"
    
    if table_name == 'blocks':
        return f"{base_path}/blocks/{filename}"
    elif table_name.endswith('_events'):
        event_type = table_name.replace('_events', '').lower()
        return f"{base_path}/events/{event_type}/{filename}"
    else:
        return f"{base_path}/{table_name}/{filename}" 