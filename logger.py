"""
OpenList-strm Logger
Provides consistent logging across app.py and main.py.
"""
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name='app', log_dir='logs', level=logging.INFO):
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name: Logger name (used to name the log file)
        log_dir: Directory to store log files
        level: Logging level
    
    Returns:
        (logger, log_file_path)
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{name}.log')
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers on re-init
    if logger.handlers:
        return logger, log_file
    
    # File handler with rotation (10MB max, keep 3 backups)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_fmt)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_fmt = logging.Formatter('[%(levelname)s] %(message)s')
    console_handler.setFormatter(console_fmt)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger, log_file
