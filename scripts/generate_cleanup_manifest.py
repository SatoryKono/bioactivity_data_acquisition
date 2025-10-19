import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class LargeFile:
    path: str
    size_bytes: int


def run_git_ls_files() -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files"], capture_output=True, text=True, check=True  # noqa: S607
        )
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        files: list[str] = []
        for root, _dirs, filenames in os.walk("."):
            # skip .git and venvs
            if ".git" in root.split(os.sep):
                continue
            if os.sep + "venv" + os.sep in root + os.sep:
                continue
            if os.sep + ".venv" + os.sep in root + os.sep:
                continue
            for name in filenames:
                files.append(os.path.normpath(os.path.join(root, name)))
        return files


def file_size_bytes(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return -1


def main() -> None:
    tracked = run_git_ls_files()

    large_files: list[LargeFile] = []
    logs: list[str] = []
    temp_files: list[str] = []
    test_outputs: list[str] = []
    pycache: list[str] = []

    for f in tracked:
        size = file_size_bytes(f)
        if size > 512_000:
            large_files.append(LargeFile(path=f, size_bytes=size))

        if f.startswith("logs/") and f.endswith(".log"):
            logs.append(f)

        name = Path(f).name
        if name.startswith("temp_"):
            temp_files.append(f)

        if f.startswith("tests/test_outputs/"):
            test_outputs.append(f)

        if "__pycache__" in f or f.endswith(".pyc") or f.endswith(".pyo"):
            pycache.append(f)

    manifest = {
        "large_files": [asdict(lf) for lf in large_files],
        "logs": logs,
        "temp_files": temp_files,
        "test_outputs": test_outputs,
        "pycache": pycache,
    }

    with open("CLEANUP_MANIFEST.json", "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, ensure_ascii=False, indent=2)

    print("CLEANUP_MANIFEST.json generated with", len(tracked), "tracked files scanned")


if __name__ == "__main__":
    main()


