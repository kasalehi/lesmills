import sys
from pathlib import Path
import dill

from src.les.exception import CustomException
from src.les.logger import logging

# global artifacts path inside Astro container
ARTIFACTS_DIR = Path("/usr/local/airflow/include/artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


# -------- save model --------
def save_model(obj, model_path: str = "model.pkl"):
    try:
        path_model = ARTIFACTS_DIR / model_path

        with open(path_model, "wb") as f:
            dill.dump(obj, f)

        logging.info(f"Model saved to {path_model}")

        return path_model

    except Exception as e:
        raise CustomException(e, sys)


# -------- save preprocessor --------
def save_preprocessor(obj, pre_path: str = "preprocessor.pkl"):
    try:
        path_preprocessor = ARTIFACTS_DIR / pre_path

        with open(path_preprocessor, "wb") as f:
            dill.dump(obj, f)

        logging.info(f"Preprocessor saved to {path_preprocessor}")

        return path_preprocessor

    except Exception as e:
        raise CustomException(e, sys)


# -------- load model --------
def load_model(filename: str = "model.pkl"):
    try:
        path_model = ARTIFACTS_DIR / filename

        with open(path_model, "rb") as f:
            model = dill.load(f)

        logging.info(f"Model loaded from {path_model}")

        return model

    except Exception as e:
        raise CustomException(e, sys)


# -------- load preprocessor --------
def load_preprocessor(filename: str = "preprocessor.pkl"):
    try:
        path_preprocessor = ARTIFACTS_DIR / filename

        with open(path_preprocessor, "rb") as f:
            preprocessor = dill.load(f)

        logging.info(f"Preprocessor loaded from {path_preprocessor}")

        return preprocessor

    except Exception as e:
        raise CustomException(e, sys)