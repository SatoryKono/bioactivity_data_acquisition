"""Script to remove unused type: ignore comments from activity.py"""

import re
from pathlib import Path

# Target file
file_path = Path("src/bioetl/pipelines/chembl/activity.py")

# Lines where mypy reports unused type: ignore comments
target_lines = [
    43, 80, 94, 96, 99, 111, 119, 125, 128, 144, 150, 157, 160, 169, 179, 182,
    190, 193, 194, 195, 197, 205, 238, 253, 259, 278, 280, 281, 285, 294,
    346, 347, 348, 350, 351, 353, 359, 360, 361, 363, 364, 366, 369, 385, 388,
    391, 395, 398, 402, 403, 406, 415, 416, 418, 420, 421, 422, 424, 427, 428,
    430, 432, 433, 434, 436, 466, 467, 469, 471, 474, 494, 497, 499, 521, 523,
    528, 543, 566, 568, 570, 576, 594, 595, 596, 597, 600, 608, 614, 615, 627,
    638, 642, 643, 646, 647, 648, 649, 653, 661, 665, 677, 678, 682, 686, 690,
    708, 713, 719, 745, 746, 751, 755, 757, 760, 761, 764, 765, 768, 769, 772,
    773, 775, 778, 796, 806, 808, 809, 810, 811, 827, 841, 858, 860, 879, 909,
]

# Read file
with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Pattern to match type: ignore comments (including variants with error codes)
pattern = re.compile(r"\s*#\s*type:\s*ignore[^\n]*", re.IGNORECASE)

modified = False
for line_num in target_lines:
    idx = line_num - 1
    if idx < len(lines):
        original = lines[idx]
        # Remove type: ignore comments from the line
        cleaned = pattern.sub("", original)
        if cleaned != original:
            lines[idx] = cleaned
            modified = True
            print(f"Line {line_num}: Removed comment from: {original.rstrip()[:80]}")

if modified:
    # Write back
    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"\nRemoved type: ignore comments from {file_path}")
else:
    print(f"\nNo type: ignore comments found on target lines in {file_path}")

