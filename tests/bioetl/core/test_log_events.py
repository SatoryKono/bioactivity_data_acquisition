import re

from bioetl.core.logging import LogEvents


def test_log_event_values_format() -> None:
    pattern = re.compile(r"^[a-z0-9_.-]+$")
    for event in LogEvents:
        assert pattern.fullmatch(event.value)
        assert event.value.count(".") >= 2
