import pytest
from ai_agent.detection import ImageDetector, DetectorConfig


def test_detector_config_defaults():
    cfg = DetectorConfig()
    assert cfg.model_name.endswith('pt')


@pytest.mark.skip(reason="requires YOLO weights and environment")
def test_image_detector_loads(tmp_path):
    # this test will only run if ultralytics is installed and model file exists
    detector = ImageDetector()
    assert detector.model is not None
