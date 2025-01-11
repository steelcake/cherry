from pathlib import Path
from datetime import datetime
import logging, sys, codecs

# Global variable to track if logging has been set up
_is_logging_configured = False
# Global variable to store the current log file path
_current_log_file = None

def setup_logging():
    """Configure logging for all modules"""
    global _is_logging_configured, _current_log_file
    
    # If logging is already configured, return the existing logger
    if _is_logging_configured:
        return logging.getLogger(__name__)

    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    _current_log_file = log_dir / f"blockchain_ingestion_{timestamp}.log"

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
    root_logger.handlers = []  # Clear any existing handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Disable debug logging for some noisy libraries
    logging.getLogger('urllib3').setLevel(logging.INFO)
    logging.getLogger('web3').setLevel(logging.INFO)
    logging.getLogger('asyncio').setLevel(logging.INFO)
    
    # Mark logging as configured
    _is_logging_configured = True
    
    return logging.getLogger(__name__)

def get_current_log_file() -> Path:
    """Get the path to the current log file"""
    return _current_log_file 