import logging
from pathlib import Path
from datetime import datetime

LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

logs_dir = Path(__file__).resolve().parent.parent.parent/"logs"
logs_dir.mkdir(parents=True, exist_ok=True)

log_path_file = logs_dir / LOG_FILE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler(log_path_file)
    formatter = logging.Formatter(
        "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

logger.info("Logger initialized")