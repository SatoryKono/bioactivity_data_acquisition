import pandas as pd

from project.library.utils.joins import safe_left_join


def test_safe_left_join_preserves_suffixed_columns():
    left = pd.DataFrame({
        "id": [1, 2],
        "value": [10, 20],
    })
    right = pd.DataFrame({
        "id": [1, 2],
        "value": [100, 200],
        "extra": ["a", "b"],
    })

    result = safe_left_join(left, right, on="id")

    assert list(result.columns) == ["id", "value_left", "value_right", "extra"]
    assert result["value_left"].tolist() == [10, 20]
    assert result["value_right"].tolist() == [100, 200]
    assert result["extra"].tolist() == ["a", "b"]
