import logging
import sys
from datetime import datetime
from pathlib import Path

# Global variables to track logging state
_is_logging_configured = False
_current_log_file = None

def setup_logging():
    """Configure logging for all modules"""
    global _is_logging_configured, _current_log_file
    
    # If logging is already configured, return the existing logger
    if _is_logging_configured:
        return logging.getLogger(__name__)

    # Create logs directory in current directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    _current_log_file = log_dir / f"blockchain_etl_{timestamp}.log"

    # Configure logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler(_current_log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler with proper encoding for Windows
    if sys.platform == 'win32':
        # Fix Windows console encoding
        sys.stdout.reconfigure(encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Set higher log level for noisy third-party libraries
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    logging.getLogger('aiobotocore').setLevel(logging.WARNING)
    logging.getLogger('aioboto3').setLevel(logging.WARNING)
    logging.getLogger("awswrangler").setLevel(logging.WARNING)

    _is_logging_configured = True
    return root_logger

def get_current_log_file() -> Path:
    """Get the path to the current log file"""
    return _current_log_file 