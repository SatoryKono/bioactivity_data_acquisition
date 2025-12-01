"""Команда для генерации диаграмм объектов пакетов."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from shutil import copy2, which

ROOT_PKG = "bioetl"
OUTDIR_NAME = "diagrams"
DPI = 300
RANKDIR = "LR"
PYREVERSE_OPTS = ["-AS"]  # show attributes and associations


def _check_tools() -> None:
    """Проверяет наличие необходимых инструментов."""
    if which("pyreverse") is None:
        msg = "Не найден 'pyreverse' (установите pylint: pip install pylint)"
        raise RuntimeError(msg)
    if which("dot") is None:
        msg = (
            "Не найден 'dot' (Graphviz). "
            "Установите graphviz и убедитесь, что 'dot' в PATH."
        )
        raise RuntimeError(msg)


def _has_bioetl_at(path: Path) -> bool:
    """Проверяет наличие bioetl/__init__.py в указанном пути."""
    bioetl_dir = path / ROOT_PKG
    init_py = bioetl_dir / "__init__.py"
    return bioetl_dir.is_dir() and init_py.is_file()


def _normalize_repo_candidate(candidate: Path) -> Path:
    """Нормализует путь к корню репозитория."""
    candidate = candidate.resolve()
    if candidate.name.lower() == "src":
        return candidate.parent
    return candidate


def find_repo_root(provided_root: Path | None = None) -> Path:
    """Находит корень репозитория с пакетом bioetl."""
    if provided_root:
        cand = Path(provided_root).resolve()
        if cand.name.lower() == "src":
            if _has_bioetl_at(cand):
                return cand.parent
            cand = cand.parent
        if _has_bioetl_at(cand):
            return _normalize_repo_candidate(cand)
        msg = f"В указанной папке '{provided_root}' не найдена '{ROOT_PKG}/__init__.py'."
        raise ValueError(msg)

    # Ищем от текущей директории
    cur = Path.cwd()
    for _ in range(10):  # Ограничиваем глубину поиска
        if _has_bioetl_at(cur):
            return _normalize_repo_candidate(cur)
        src_candidate = cur / "src"
        if src_candidate.is_dir() and _has_bioetl_at(src_candidate):
            return cur
        parent = cur.parent
        if parent == cur:
            break
        cur = parent

    msg = (
        f"Не удалось найти корень репозитория с пакетом '{ROOT_PKG}'.\n"
        "Убедитесь, что вы находитесь в корне проекта или укажите --repo-root."
    )
    raise ValueError(msg)


def find_packages(root_pkg_path: Path) -> list[str]:
    """Находит все подпакеты в bioetl."""
    pkgs: list[str] = []
    for root, dirs, files in os.walk(root_pkg_path):
        if any(part.startswith(".") for part in Path(root).parts):
            continue
        if "__init__.py" in files:
            rel = Path(root).relative_to(root_pkg_path)
            if rel == Path("."):
                pkg = ROOT_PKG
            else:
                parts = rel.parts
                if any(
                    p.lower().startswith("test") or p.lower() == "tests"
                    for p in parts
                ):
                    continue
                pkg = ROOT_PKG + "." + ".".join(parts)
            if "test" in pkg.lower():
                continue
            pkgs.append(pkg)
    return sorted(set(pkgs))


def run_pyreverse(pkg: str, project_name: str, repo_root: Path) -> None:
    """Запускает pyreverse для указанного пакета."""
    env = os.environ.copy()
    py_path = str(repo_root)
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = py_path + os.pathsep + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = py_path
    cmd = ["pyreverse", "-o", "dot", "-p", project_name] + PYREVERSE_OPTS + [pkg]
    subprocess.run(cmd, check=True, env=env, cwd=repo_root)


def _pkg_short_name(pkg_full: str) -> str:
    """Возвращает короткое имя пакета для использования в путях."""
    last = pkg_full.split(".")[-1]
    safe = re.sub(r"[^0-9A-Za-z_.-]", "_", last)
    return safe or "pkg"


def find_specific_dotfiles(repo_root: Path, project_name: str) -> tuple[Path | None, Path | None]:
    """Находит сгенерированные .dot файлы."""
    class_candidates = [
        repo_root / f"classes_{project_name}.dot",
        repo_root / "classes.dot",
    ]
    package_candidates = [
        repo_root / f"packages_{project_name}.dot",
        repo_root / "packages.dot",
    ]

    class_dot = next((c for c in class_candidates if c.exists()), None)
    if class_dot is None:
        for f in repo_root.glob("classes_*.dot"):
            if project_name in f.stem:
                class_dot = f
                break

    package_dot = next((c for c in package_candidates if c.exists()), None)
    if package_dot is None:
        for f in repo_root.glob("packages_*.dot"):
            if project_name in f.stem:
                package_dot = f
                break

    return class_dot, package_dot


def clean_class_dot(infile: Path, outfile: Path) -> None:
    """Очищает .dot файл от приватных классов и тестов."""
    node_decl_re = re.compile(r'^\s*"(?P<name>[^"]+)"\s*\[.*\];\s*$')
    remove_names: set[str] = set()

    with infile.open(encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        m = node_decl_re.match(line)
        if not m:
            continue
        fullname = m.group("name")
        simple = fullname.split(".")[-1]
        parts = fullname.split(".")
        low = fullname.lower()
        if (
            simple.startswith("_")
            or any(p.startswith("test") or p == "tests" for p in parts)
            or ".test." in low
            or ".tests." in low
        ):
            remove_names.add(fullname)

    with outfile.open("w", encoding="utf-8") as out:
        for line in lines:
            skip = any(f'"{rn}"' in line for rn in remove_names)
            if not skip:
                out.write(line)

    # Добавляем rankdir если отсутствует
    with outfile.open("r", encoding="utf-8") as f:
        txt = f.read()
    if "rankdir" not in txt:
        txt = re.sub(
            r'^(digraph\s+"[^"]+"\s*\{\s*)',
            rf'\1\n    rankdir={RANKDIR};\n',
            txt,
            flags=re.MULTILINE,
        )
        with outfile.open("w", encoding="utf-8") as f:
            f.write(txt)


def clean_package_dot(infile: Path, outfile: Path) -> None:
    """Очищает .dot файл пакетов от тестовых модулей."""
    node_decl_re = re.compile(r'^\s*"(?P<name>[^"]+)"\s*\[.*\];\s*$')
    remove_names: set[str] = set()

    with infile.open(encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        m = node_decl_re.match(line)
        if not m:
            continue
        fullname = m.group("name")
        parts = fullname.split(".")
        low = fullname.lower()
        if (
            any(p.startswith("test") or p == "tests" for p in parts)
            or ".test." in low
            or ".tests." in low
        ):
            remove_names.add(fullname)

    with outfile.open("w", encoding="utf-8") as out:
        for line in lines:
            skip = any(f'"{rn}"' in line for rn in remove_names)
            if not skip:
                out.write(line)

    # Добавляем rankdir если отсутствует
    with outfile.open("r", encoding="utf-8") as f:
        txt = f.read()
    if "rankdir" not in txt:
        txt = re.sub(
            r'^(digraph\s+"[^"]+"\s*\{\s*)',
            rf'\1\n    rankdir={RANKDIR};\n',
            txt,
            flags=re.MULTILINE,
        )
        with outfile.open("w", encoding="utf-8") as f:
            f.write(txt)


def render_dot_to_files(dotfile: Path, svg_out: Path, png_out: Path) -> None:
    """Рендерит .dot файл в SVG и PNG."""
    env = os.environ.copy()
    subprocess.run(
        [
            "dot",
            "-Tsvg",
            f"-Gdpi={DPI}",
            f"-Grankdir={RANKDIR}",
            str(dotfile),
            "-o",
            str(svg_out),
        ],
        check=True,
        env=env,
    )
    subprocess.run(
        [
            "dot",
            "-Tpng",
            f"-Gdpi={DPI}",
            f"-Grankdir={RANKDIR}",
            str(dotfile),
            "-o",
            str(png_out),
        ],
        check=True,
        env=env,
    )


def generate_diagrams(
    *,
    package: str | None = None,
    repo_root: Path | None = None,
    output_dir: Path | None = None,
) -> None:
    """Генерирует диаграммы классов и пакетов.

    Args:
        package: Имя пакета для генерации (например, 'bioetl.clients').
                 Если None, генерирует для всех пакетов.
        repo_root: Корень репозитория. Если None, определяется автоматически.
        output_dir: Директория для сохранения диаграмм. Если None, используется
                    'diagrams' в корне репозитория.
    """
    _check_tools()

    repo_root_path = find_repo_root(repo_root)
    if output_dir is None:
        output_dir = repo_root_path / OUTDIR_NAME
    else:
        output_dir = Path(output_dir).resolve()

    # Определяем путь к пакету bioetl
    bioetl_path = repo_root_path / ROOT_PKG
    if not bioetl_path.is_dir():
        alt = repo_root_path / "src" / ROOT_PKG
        if alt.is_dir():
            bioetl_path = alt

    if not bioetl_path.is_dir():
        msg = f"Папка {bioetl_path} не найдена (ожидается bioetl/ или src/bioetl)"
        raise ValueError(msg)

    # Находим пакеты для генерации
    if package:
        pkgs = [package]
    else:
        pkgs = find_packages(bioetl_path)
        if not pkgs:
            msg = "Не найдено подпакетов внутри 'bioetl'."
            raise ValueError(msg)

    # Создаем выходные директории
    dot_root = output_dir / "dot"
    svg_root = output_dir / "svg"
    png_root = output_dir / "png"
    dot_root.mkdir(parents=True, exist_ok=True)
    svg_root.mkdir(parents=True, exist_ok=True)
    png_root.mkdir(parents=True, exist_ok=True)

    # Генерируем диаграммы для каждого пакета
    for pkg in pkgs:
        project_name = pkg.replace(".", "_")
        pkg_short = _pkg_short_name(pkg)

        # Создаем поддиректории для пакета
        dot_dir = dot_root / pkg_short
        svg_dir = svg_root / pkg_short
        png_dir = png_root / pkg_short
        dot_dir.mkdir(exist_ok=True)
        svg_dir.mkdir(exist_ok=True)
        png_dir.mkdir(exist_ok=True)

        try:
            run_pyreverse(pkg, project_name, repo_root_path)
        except subprocess.CalledProcessError as e:
            print(f"  Ошибка pyreverse для {pkg}: {e}")
            continue

        class_dot, package_dot = find_specific_dotfiles(repo_root_path, project_name)

        # Обрабатываем диаграмму классов
        if class_dot:
            try:
                class_copy = dot_dir / class_dot.name
                copy2(class_dot, class_copy)
                class_clean = dot_dir / f"{project_name}.classes.clean.dot"
                clean_class_dot(class_copy, class_clean)
                svg_out = svg_dir / f"{project_name}_classes.svg"
                png_out = png_dir / f"{project_name}_classes.png"
                render_dot_to_files(class_clean, svg_out, png_out)
            except Exception as e:
                print(f"  Ошибка при обработке class-dot для {pkg}: {e}")

        # Обрабатываем диаграмму пакетов
        if package_dot:
            try:
                pkg_copy = dot_dir / package_dot.name
                copy2(package_dot, pkg_copy)
                pkg_clean = dot_dir / f"{project_name}.packages.clean.dot"
                clean_package_dot(pkg_copy, pkg_clean)
                svg_out = svg_dir / f"{project_name}_packages.svg"
                png_out = png_dir / f"{project_name}_packages.png"
                render_dot_to_files(pkg_clean, svg_out, png_out)
            except Exception as e:
                print(f"  Ошибка при обработке package-dot для {pkg}: {e}")

    print(f"Генерация завершена. Файлы находятся в: {output_dir}")
    print(f"  - DOT root: {dot_root}")
    print(f"  - SVG root: {svg_root}")
    print(f"  - PNG root: {png_root}")


__all__ = ["generate_diagrams"]

