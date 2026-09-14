import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a standardized logger.
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate logs if logger is fetched multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [Module] - Message
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    return logger