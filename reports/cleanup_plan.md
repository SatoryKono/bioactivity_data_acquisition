## A. Удаление (confidence=high)

| Object | File | Reason | Verification |
| --- | --- | --- | --- |
| orphan_helper | src/bioetl/legacy/orphan_helper.py | No usages after pipeline refactor | rg "orphan_helper" src tests<br>rg -g '*.{yaml,json}' "orphan_helper" configs<br>pytest |

## B. Депрекация (confidence=medium)

| Object | File | Reason | Verification |
| --- | --- | --- | --- |
| maybe_unused | src/bioetl/utils/dead_code_example.py | Only referenced in legacy docs | rg "maybe_unused" src tests<br>rg "maybe_unused" docs<br>rg -g '*.{yaml,json}' "maybe_unused" configs |

## C. Докстринг-комментарий (low/other)

| Object | File | Reason | Verification |
| --- | --- | --- | --- |
| legacy_stub | src/bioetl/utils/dead_code_example.py | Placeholder for removed API | rg "legacy_stub" src tests |
