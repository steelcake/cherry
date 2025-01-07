from pathlib import Path
import logging
import sys
from datetime import datetime

def setup_logging():
    """Configure logging for all modules"""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"blockchain_ingestion_{timestamp}.log"

    # Configure logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler - set to DEBUG level to capture all details
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler - keep at INFO level for cleaner console output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Disable debug logging for some noisy libraries
    logging.getLogger('urllib3').setLevel(logging.INFO)
    logging.getLogger('web3').setLevel(logging.INFO)
    logging.getLogger('asyncio').setLevel(logging.INFO)
    
    return logging.getLogger(__name__) 