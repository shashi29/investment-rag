import logging
import sys
from pathlib import Path
from typing import Optional
import yaml
from datetime import datetime

def setup_logger(
    name: str,
    log_level: str = "INFO",
    log_file: Optional[str] = None
) -> logging.Logger:
    """Set up logger with specified configuration."""
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if log file specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger

def load_logging_config(config_path: str) -> dict:
    """Load logging configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading logging config: {str(e)}")
        return {}

def get_logger(
    name: str,
    config_path: Optional[str] = None
) -> logging.Logger:
    """Get logger instance with optional configuration."""
    if config_path:
        config = load_logging_config(config_path)
        if config:
            logging.config.dictConfig(config)
            return logging.getLogger(name)
    
    # Default configuration if no config file provided
    log_file = f"logs/{datetime.now().strftime('%Y-%m-%d')}.log"
    return setup_logger(name, log_file=log_file)

class LoggerAdapter(logging.LoggerAdapter):
    """Custom logger adapter for adding context to log messages."""
    
    def process(self, msg, kwargs):
        """Process the logging message and keyword arguments."""
        extra = kwargs.get("extra", {})
        if self.extra:
            extra.update(self.extra)
        
        # Add timestamp to extra
        extra["timestamp"] = datetime.now()
        
        # Format message with extra context
        if extra:
            msg = f"{msg} - Context: {extra}"
        
        return msg, kwargs

def create_structured_logger(name: str) -> LoggerAdapter:
    """Create a structured logger with additional context handling."""
    logger = get_logger(name)
    return LoggerAdapter(logger, extra={"service": name})

# Error logging specific functions
def log_error_with_context(
    logger: logging.Logger,
    error: Exception,
    context: dict
) -> None:
    """Log error with additional context information."""
    error_details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "context": context,
        "timestamp": datetime.now()
    }
    logger.error(f"Error occurred: {error_details}")

def log_api_error(
    logger: logging.Logger,
    provider: str,
    endpoint: str,
    error: Exception,
    response: Optional[dict] = None
) -> None:
    """Log API-specific errors."""
    error_details = {
        "provider": provider,
        "endpoint": endpoint,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "response": response,
        "timestamp": datetime.now()
    }
    logger.error(f"API Error: {error_details}")

# Performance logging
def log_performance_metric(
    logger: logging.Logger,
    operation: str,
    duration: float,
    metadata: Optional[dict] = None
) -> None:
    """Log performance metrics."""
    metric_details = {
        "operation": operation,
        "duration_ms": duration * 1000,
        "metadata": metadata or {},
        "timestamp": datetime.now()
    }
    logger.info(f"Performance metric: {metric_details}")