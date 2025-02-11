import logging
from typing import Optional, List, Dict, Any, Tuple
from hypersync import (
    Query, StreamConfig, LogSelection, BlockSelection,
    HexOutput, ColumnMapping, DataType,
    BlockField, TransactionField, LogField,
    signature_to_topic0, FieldSelection, JoinMode
)

logger = logging.getLogger(__name__)

def _convert_type(type_str: str) -> DataType:
    """Convert string type to Hypersync DataType"""
    type_map = {
        'int64': DataType.INT64,
        'int32': DataType.INT32,
        'uint64': DataType.UINT64,
        'uint32': DataType.UINT32,
        'float64': DataType.FLOAT64,
        'float32': DataType.FLOAT32,
        'string': DataType.STRING,
        'intstr': DataType.INTSTR
    }
    return type_map.get(type_str.lower(), DataType.INTSTR)

def _get_block_column_mapping(column_mapping: Optional[Dict[str, str]] = None) -> Dict[BlockField, DataType]:
    """Get block column mapping with custom overrides"""
    default_mapping = {
        BlockField.DIFFICULTY: DataType.INTSTR,
        BlockField.TOTAL_DIFFICULTY: DataType.INTSTR,
        BlockField.SIZE: DataType.INTSTR,
        BlockField.GAS_LIMIT: DataType.INTSTR,
        BlockField.GAS_USED: DataType.INTSTR,
        BlockField.TIMESTAMP: DataType.INT64,
        BlockField.BASE_FEE_PER_GAS: DataType.INTSTR,
        BlockField.BLOB_GAS_USED: DataType.INTSTR,
        BlockField.EXCESS_BLOB_GAS: DataType.INTSTR,
        BlockField.SEND_COUNT: DataType.INTSTR,
    }
    
    if column_mapping:
        for field, type_str in column_mapping.items():
            try:
                block_field = BlockField(field)
                default_mapping[block_field] = _convert_type(type_str)
            except ValueError:
                logger.warning(f"Invalid block field: {field}")
                
    return default_mapping

def _get_transaction_column_mapping(column_mapping: Optional[Dict[str, str]] = None) -> Dict[TransactionField, DataType]:
    """Get transaction column mapping with custom overrides"""
    default_mapping = {
        TransactionField.GAS: DataType.INTSTR,
        TransactionField.GAS_PRICE: DataType.INTSTR,
        TransactionField.NONCE: DataType.INTSTR,
        TransactionField.VALUE: DataType.INTSTR,
        TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.INTSTR,
        TransactionField.MAX_FEE_PER_GAS: DataType.INTSTR,
        TransactionField.CHAIN_ID: DataType.INT64,
        TransactionField.EFFECTIVE_GAS_PRICE: DataType.INTSTR,
        TransactionField.GAS_USED: DataType.INTSTR,
        TransactionField.CUMULATIVE_GAS_USED: DataType.INTSTR,
        TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.INTSTR,
    }
    
    if column_mapping:
        for field, type_str in column_mapping.items():
            try:
                tx_field = TransactionField(field)
                default_mapping[tx_field] = _convert_type(type_str)
            except ValueError:
                logger.warning(f"Invalid transaction field: {field}")
                
    return default_mapping

def generate_block_query(
    from_block: int,
    to_block: Optional[int] = None,
    include_transactions: bool = True,
    include_logs: bool = False,
    column_mapping: Optional[Dict[str, Dict[str, str]]] = None
) -> Tuple[Query, StreamConfig]:
    """Generate Hypersync query for block data"""
    # Build field selection
    field_selection = FieldSelection(
        block=[field.value for field in BlockField],
        transaction=[field.value for field in TransactionField] if include_transactions else None,
        log=[field.value for field in LogField] if include_logs else None
    )
    
    # Create query
    query = Query(
        from_block=from_block,
        to_block=None if to_block is None else to_block + 1,
        join_mode=JoinMode.JOIN_ALL if include_transactions else None,
        blocks=[BlockSelection()],
        field_selection=field_selection
    )
    
    # Build column mappings
    block_mapping = _get_block_column_mapping(
        column_mapping.get('block') if column_mapping else None
    )
    
    transaction_mapping = None
    if include_transactions:
        transaction_mapping = _get_transaction_column_mapping(
            column_mapping.get('transaction') if column_mapping else None
        )
    
    # Create stream config
    stream_config = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        column_mapping=ColumnMapping(
            block=block_mapping,
            transaction=transaction_mapping
        )
    )
    
    logger.info(
        f"Generated block query:\n"
        f"  From block: {from_block}\n"
        f"  To block: {to_block}\n"
        f"  Include transactions: {include_transactions}\n"
        f"  Include logs: {include_logs}\n"
        f"  Column mappings: {column_mapping}"
    )
    
    return query, stream_config

def generate_event_query(
    signature: str,
    from_block: int,
    to_block: Optional[int] = None,
    addresses: Optional[List[str]] = None,
    include_transactions: bool = False,
    column_mapping: Optional[Dict[str, str]] = None
) -> Tuple[Query, StreamConfig]:
    """Generate Hypersync query for event data"""
    # Get event signature hash
    topic0 = signature_to_topic0(signature)
    
    # Build field selection
    field_selection = FieldSelection(
        log=[field.value for field in LogField],
        block=[BlockField.NUMBER.value, BlockField.TIMESTAMP.value],
        transaction=[TransactionField.HASH.value, TransactionField.FROM.value] if include_transactions else None
    )
    
    # Create query
    query = Query(
        from_block=from_block,
        to_block=None if to_block is None else to_block + 1,
        logs=[LogSelection(
            address=addresses,
            topics=[[topic0]]
        )],
        field_selection=field_selection
    )
    
    # Build decoded log mapping
    decoded_mapping = {}
    if column_mapping:
        for field, type_str in column_mapping.items():
            decoded_mapping[field] = _convert_type(type_str)
    
    # Create stream config
    stream_config = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        event_signature=signature,
        column_mapping=ColumnMapping(
            decoded_log=decoded_mapping,
            block={BlockField.TIMESTAMP: DataType.INT64}
        )
    )
    
    logger.info(
        f"Generated event query for {signature}:\n"
        f"  From block: {from_block}\n"
        f"  To block: {to_block}\n"
        f"  Topics: [[{topic0}]]\n"
        f"  Addresses: {addresses or []}\n"
        f"  Include transactions: {include_transactions}\n"
        f"  Column mapping: {decoded_mapping}"
    )
    
    return query, stream_config