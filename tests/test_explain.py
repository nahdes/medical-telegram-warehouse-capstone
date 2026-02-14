from ai_agent.explain import compute_dummy_shap

def test_compute_dummy_shap():
    X, shap_values = compute_dummy_shap()
    assert X.shape[0] == 100
    assert hasattr(shap_values, 'values')
