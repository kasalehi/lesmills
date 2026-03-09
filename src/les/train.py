import sys
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field

from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    balanced_accuracy_score,
)
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.utils.class_weight import compute_sample_weight

from src.les.logger import logging
from src.les.exception import CustomException
from src.les.ustils import load_preprocessor, save_model


@dataclass
class ModelConfig:
    base_dir: str = Path(__file__).resolve().parent.parent.parent / "artifacts"
    preprocessor_path: str = base_dir / "preprocessor.pkl"

    param_grids: dict = field(default_factory=lambda: {
        "random_forest": {
            "clf__n_estimators": [200, 400],
            "clf__max_depth": [None, 5, 10],
            "clf__min_samples_split": [2, 5],
            "clf__min_samples_leaf": [1, 2],
            "clf__class_weight": ["balanced", "balanced_subsample"],
        },
        "gradient_boosting": {
            "clf__n_estimators": [100, 200],
            "clf__learning_rate": [0.01, 0.1],
            "clf__max_depth": [3, 5],
        },
    })

    models: dict = field(default_factory=lambda: {
        "random_forest": RandomForestClassifier(random_state=42),
        "gradient_boosting": GradientBoostingClassifier(random_state=42),
    })


class ModelTraing:
    def __init__(self):
        self.data = ModelConfig()
    def trainingModel(self, x_train, y_train, x_test, y_test, preprocessor):
        try:
            results = {}
            cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

            for name, model in self.data.models.items():
                logging.info(f"Training model: {name}")

                param_grid = self.data.param_grids.get(name, {})
                if not param_grid:
                    logging.warning(f"No param grid found for model '{name}'. Skipping.")
                    continue

                # Pipeline ensures preprocessing happens inside CV folds (no leakage)
                pipe = Pipeline([
                    ("preprocess", preprocessor),
                    ("clf", model),
                ])

                grid = GridSearchCV(
                    estimator=pipe,
                    param_grid=param_grid,
                    cv=cv,
                    n_jobs=2,
                    scoring="f1_macro",
                    verbose=1,
                )

                fit_params = {}
                if name == "gradient_boosting":
                    # handle imbalance via sample weights
                    sw = compute_sample_weight(class_weight="balanced", y=y_train)
                    fit_params["clf__sample_weight"] = sw

                grid.fit(x_train, y_train, **fit_params)

                best_estimator = grid.best_estimator_
                best_params = grid.best_params_
                cv_score = grid.best_score_

                y_pred = best_estimator.predict(x_test)

                test_acc = accuracy_score(y_test, y_pred)
                test_bal_acc = balanced_accuracy_score(y_test, y_pred)
                test_f1_macro = f1_score(y_test, y_pred, average="macro")

                logging.info(
                    f"Model: {name} | Best Params: {best_params} | "
                    f"CV f1_macro: {cv_score:.4f} | "
                    f"Test Acc: {test_acc:.4f} | "
                    f"Test BalAcc: {test_bal_acc:.4f} | "
                    f"Test F1_macro: {test_f1_macro:.4f}"
                )
                logging.info("Confusion Matrix:\n" + str(confusion_matrix(y_test, y_pred)))
                logging.info("Classification Report:\n" + classification_report(y_test, y_pred))

                results[name] = {
                    "best_estimator": best_estimator,
                    "best_params": best_params,
                    "cv_score": cv_score,
                    "test_accuracy": test_acc,
                    "test_balanced_accuracy": test_bal_acc,
                    "test_f1_macro": test_f1_macro,
                }

            if not results:
                raise CustomException("No models were successfully trained.", sys)

            best_model_name = max(results.keys(), key=lambda m: results[m]["test_f1_macro"])
            best_info = results[best_model_name]

            logging.info(
                f"Best model is '{best_model_name}' "
                f"with test F1_macro {best_info['test_f1_macro']:.4f}"
            )

            saved = save_model(best_info["best_estimator"], model_path="model.pkl")
            logging.info(f"Best model saved to {saved}")

            print({
                "best_model_name": best_model_name,
                "best_params": best_info["best_params"],
                "cv_score_f1_macro": best_info["cv_score"],
                "test_accuracy": best_info["test_accuracy"],
                "test_balanced_accuracy": best_info["test_balanced_accuracy"],
                "test_f1_macro": best_info["test_f1_macro"],
            })

            return best_info["best_estimator"], best_model_name, results

        except Exception as e:
            raise CustomException(e, sys)
