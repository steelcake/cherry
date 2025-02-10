from datetime import datetime

def get_output_path(base_path: str, table_name: str, start_block: int, end_block: int) -> str:
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