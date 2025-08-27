# logger_config.py
import logging
import os
from datetime import datetime

def setup_logger():
    """
    Setup logging configuration for ETL process
    """
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler('logs/etl.log', encoding='utf-8'),
            # Optionally add console handler if you want logs in console too
            # logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger('ETL')
    return logger