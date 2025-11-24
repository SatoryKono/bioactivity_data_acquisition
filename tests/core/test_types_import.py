import importlib


def test_types_import_is_available():
    module = importlib.import_module("bioetl.core.pipeline.types")

    assert hasattr(module, "RunResult")
    assert hasattr(module, "StageContext")
    assert module.PipelineExtractionMode.AUTO.value == "auto"
