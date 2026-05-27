import sys
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from collections import Counter

from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import (
accuracy_score,
classification_report,
confusion_matrix,
f1_score,
balanced_accuracy_score,
roc_auc_score,
recall_score,
precision_score,
make_scorer,
)
from sklearn.utils.class_weight import compute_class_weight
from sklearn.ensemble import GradientBoostingClassifier
from xgboost import XGBClassifier

from src.les.logger import logging
from src.les.exception import CustomException
from src.les.ustils import save_model

# Encoded class 0 = original Churned=1 (imminent, 0-3 months).
# This is the class we prioritize for recall.
IMMINENT_CLASS = 0
# If P(imminent) >= this threshold, override argmax to predict imminent.
# Lower = higher recall, lower precision.
IMMINENT_THRESHOLD = 0.25
# Extra multiplier on top of 'balanced' class weights for the imminent class.
IMMINENT_WEIGHT_BOOST = 2.0

@dataclass
class ModelConfig:
        base_dir: str = Path(__file__).resolve().parent.parent.parent / "artifacts"
        preprocessor_path: str = base_dir / "preprocessor.pkl"


        # ❌ RandomForest REMOVED
        param_grids: dict = field(default_factory=lambda: {
            "gradient_boosting": {
                "clf__n_estimators": [200, 300],
                "clf__learning_rate": [0.05, 0.1],
                "clf__max_depth": [3, 5],
                "clf__min_samples_leaf": [1, 5],
            },
            "xgboost": {
                "clf__n_estimators": [300, 500],
                "clf__max_depth": [4, 6],
                "clf__learning_rate": [0.05, 0.1],
                "clf__min_child_weight": [1, 5],
            },
        })

        models: dict = field(default_factory=lambda: {
            "gradient_boosting": GradientBoostingClassifier(random_state=42),
            "xgboost":XGBClassifier(
                random_state=42,
                objective="multi:softprob",
                num_class=3,
                eval_metric="mlogloss",
                subsample=0.8,
                colsample_bytree=0.8
            ),
        })


class ModelTraing:
    def __init__(self):
        self.data = ModelConfig()


# ✅ FIXED INDENTATION (VERY IMPORTANT)
    def trainingModel(self, x_train, y_train, x_test, y_test, preprocessor):
        try:
            results = {}
            cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

            # ================================
            # HANDLE IMBALANCE
            # ================================
            # Use 'balanced' weights derived from actual frequencies, then boost
            # the imminent class so the model leans hard toward catching it.
            classes_sorted = np.array(sorted(set(y_train)))
            balanced_w = compute_class_weight(
                "balanced", classes=classes_sorted, y=y_train
            )
            class_weights = {int(c): float(w) for c, w in zip(classes_sorted, balanced_w)}
            if IMMINENT_CLASS in class_weights:
                class_weights[IMMINENT_CLASS] *= IMMINENT_WEIGHT_BOOST
            logging.info(
                f"Class counts: {Counter(y_train)} | "
                f"Class weights (imminent boosted x{IMMINENT_WEIGHT_BOOST}): {class_weights}"
            )
            sample_weights = np.array([class_weights[int(y)] for y in y_train])

            # Custom scorer: recall on the imminent class only.
            imminent_recall_scorer = make_scorer(
                recall_score,
                labels=[IMMINENT_CLASS],
                average="macro",
                zero_division=0,
            )

            for name, model in self.data.models.items():
                logging.info(f"🚀 Training model: {name}")

                param_grid = self.data.param_grids.get(name, {})
                if not param_grid:
                    logging.warning(f"No param grid found for model '{name}'. Skipping.")
                    continue

                pipe = Pipeline([
                    ("preprocess", preprocessor),
                    ("clf", model),
                ])

                grid = GridSearchCV(
                    estimator=pipe,
                    param_grid=param_grid,
                    cv=cv,
                    n_jobs=2,
                    scoring=imminent_recall_scorer,
                    verbose=1,
                )

                fit_params = {}

                # Apply weights
                if name in ["gradient_boosting", "xgboost"]:
                    fit_params["clf__sample_weight"] = sample_weights

                grid.fit(x_train, y_train, **fit_params)

                best_estimator = grid.best_estimator_
                best_params = grid.best_params_
                cv_score = grid.best_score_

                y_pred = best_estimator.predict(x_test)
                try:
                    y_prob = best_estimator.predict_proba(x_test)
                except Exception:
                    y_prob = None

                # Threshold-tuned predictions: override argmax with imminent
                # whenever P(imminent) >= IMMINENT_THRESHOLD.
                if y_prob is not None and IMMINENT_CLASS < y_prob.shape[1]:
                    argmax_pred = y_prob.argmax(axis=1)
                    y_pred_tuned = np.where(
                        y_prob[:, IMMINENT_CLASS] >= IMMINENT_THRESHOLD,
                        IMMINENT_CLASS,
                        argmax_pred,
                    )
                else:
                    y_pred_tuned = y_pred

                test_acc = accuracy_score(y_test, y_pred)
                test_bal_acc = balanced_accuracy_score(y_test, y_pred)
                test_f1_macro = f1_score(y_test, y_pred, average="macro")
                test_imm_recall = recall_score(
                    y_test, y_pred, labels=[IMMINENT_CLASS], average="macro", zero_division=0
                )
                test_imm_precision = precision_score(
                    y_test, y_pred, labels=[IMMINENT_CLASS], average="macro", zero_division=0
                )

                tuned_imm_recall = recall_score(
                    y_test, y_pred_tuned, labels=[IMMINENT_CLASS], average="macro", zero_division=0
                )
                tuned_imm_precision = precision_score(
                    y_test, y_pred_tuned, labels=[IMMINENT_CLASS], average="macro", zero_division=0
                )
                tuned_acc = accuracy_score(y_test, y_pred_tuned)

                try:
                    roc_auc = roc_auc_score(y_test, y_prob, multi_class="ovr") if y_prob is not None else None
                except Exception:
                    roc_auc = None

                logging.info(
                    f"Model: {name} | Best Params: {best_params} | "
                    f"CV Score (imm recall): {cv_score:.4f} | "
                    f"Test Acc: {test_acc:.4f} | "
                    f"Test BalAcc: {test_bal_acc:.4f} | "
                    f"Test F1_macro: {test_f1_macro:.4f} | "
                    f"Imm Recall (argmax): {test_imm_recall:.4f} | "
                    f"Imm Precision (argmax): {test_imm_precision:.4f} | "
                    f"Imm Recall (thr={IMMINENT_THRESHOLD}): {tuned_imm_recall:.4f} | "
                    f"Imm Precision (thr={IMMINENT_THRESHOLD}): {tuned_imm_precision:.4f} | "
                    f"Tuned Acc: {tuned_acc:.4f} | "
                    f"ROC-AUC: {roc_auc}"
                )

                logging.info("Confusion Matrix (argmax):\n" + str(confusion_matrix(y_test, y_pred)))
                logging.info("Classification Report (argmax):\n" + classification_report(y_test, y_pred, zero_division=0))
                logging.info(
                    f"Confusion Matrix (threshold={IMMINENT_THRESHOLD}):\n"
                    + str(confusion_matrix(y_test, y_pred_tuned))
                )
                logging.info(
                    f"Classification Report (threshold={IMMINENT_THRESHOLD}):\n"
                    + classification_report(y_test, y_pred_tuned, zero_division=0)
                )

                results[name] = {
                    "best_estimator": best_estimator,
                    "best_params": best_params,
                    "cv_score": cv_score,
                    "test_accuracy": test_acc,
                    "test_balanced_accuracy": test_bal_acc,
                    "test_f1_macro": test_f1_macro,
                    "test_imminent_recall": test_imm_recall,
                    "test_imminent_recall_tuned": tuned_imm_recall,
                    "test_imminent_precision_tuned": tuned_imm_precision,
                    "roc_auc": roc_auc,
                }

            if not results:
                raise CustomException("No models were successfully trained.", sys)

            # Pick the model that gives the best tuned imminent recall.
            best_model_name = max(
                results, key=lambda m: results[m]["test_imminent_recall_tuned"]
            )
            best_info = results[best_model_name]

            logging.info(
                f"🏆 Best model is '{best_model_name}' "
                f"with Imm Recall (tuned) {best_info['test_imminent_recall_tuned']:.4f} | "
                f"Imm Precision (tuned) {best_info['test_imminent_precision_tuned']:.4f} | "
                f"F1_macro {best_info['test_f1_macro']:.4f}"
            )

            saved = save_model(best_info["best_estimator"], model_path="model.pkl")
            logging.info(f"Model saved to {saved}")

            return best_info["best_estimator"], best_model_name, results

        except Exception as e:
            raise CustomException(e, sys)

