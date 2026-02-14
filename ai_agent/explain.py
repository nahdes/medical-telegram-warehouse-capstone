"""Simple integration with SHAP to produce explainability plots."""

from typing import Any, Optional

import shap
import numpy as np
import pandas as pd


def compute_dummy_shap():
    """Create dummy dataset and model and return shap values."""
    # create random data
    X = pd.DataFrame(np.random.randn(100, 5), columns=[f'feature_{i}' for i in range(5)])
    # train a simple model
    from sklearn.ensemble import RandomForestRegressor
    model = RandomForestRegressor(n_estimators=10)
    y = X.iloc[:, 0] * 2 + np.random.randn(100)
    model.fit(X, y)
    explainer = shap.Explainer(model, X)
    shap_values = explainer(X)
    return X, shap_values


def plot_summary(shap_values: Any):
    shap.summary_plot(shap_values, show=False)


def plot_force(shap_values: Any, instance: Optional[int] = 0):
    shap.force_plot(shap_values.base_values[instance], shap_values.values[instance], shap_values.data[instance], matplotlib=True)
