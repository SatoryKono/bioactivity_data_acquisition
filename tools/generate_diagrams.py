#!/usr/bin/env python3
# tools/generate_diagrams.py
# -*- coding: utf-8 -*-
"""
Generate class and package diagrams for bioetl.* packages.

Outputs (per-package subfolders):
  diagrams/
    dot/<pkg_short>/    -> original + cleaned .dot files
    svg/<pkg_short>/    -> .svg files (classes and packages)
    png/<pkg_short>/    -> .png files (classes and packages)

pkg_short = last part of package name, e.g.
  bioetl.core.schemas -> schemas
  bioetl.qc           -> qc
  bioetl              -> bioetl

Usage:
  python tools/generate_diagrams.py [--repo-root PATH]

Example:
  python tools/generate_diagrams.py --repo-root "E:\\github\\bioactivity_data_acquisition1"
"""
import os
import sys
import subprocess
import re
import argparse
from shutil import which, copy2

ROOT_PKG = 'bioetl'
OUTDIR_NAME = 'diagrams'
DPI = 300
RANKDIR = 'LR'
PYREVERSE_OPTS = ['-AS']  # show attributes and associations

def die(msg):
    print("Ошибка:", msg)
    sys.exit(1)

def check_tools():
    if which('pyreverse') is None:
        die("Не найден 'pyreverse' (установите pylint: pip install pylint)")
    if which('dot') is None:
        die("Не найден 'dot' (Graphviz). Установите graphviz и убедитесь, что 'dot' в PATH.")

def _has_bioetl_at(path):
    bioetl_dir = os.path.join(path, ROOT_PKG)
    init_py = os.path.join(bioetl_dir, '__init__.py')
    return os.path.isdir(bioetl_dir) and os.path.isfile(init_py)

def _normalize_repo_candidate(candidate_with_bioetl_parent):
    candidate = os.path.abspath(candidate_with_bioetl_parent)
    if os.path.basename(candidate).lower() == 'src':
        return os.path.dirname(candidate)
    return candidate

def find_repo_root(script_path, provided_root=None):
    """
    Find repository root containing bioetl/__init__.py.
    If bioetl is inside `src/`, returns the parent of `src`.
    """
    def search_inside_top(cand):
        if _has_bioetl_at(cand):
            return _normalize_repo_candidate(cand)
        max_depth = 4
        base_depth = cand.count(os.sep)
        for root, dirs, files in os.walk(cand):
            depth = root.count(os.sep) - base_depth
            if depth > max_depth:
                dirs[:] = []
                continue
            if ROOT_PKG in dirs and os.path.isfile(os.path.join(root, ROOT_PKG, '__init__.py')):
                return _normalize_repo_candidate(root)
        return None

    if provided_root:
        cand = os.path.abspath(provided_root)
        if os.path.basename(cand).lower() == 'src':
            if _has_bioetl_at(cand):
                return os.path.dirname(cand)
            res = search_inside_top(cand)
            if res:
                return res
            die(f"В указанной папке '{provided_root}' не найдена '{ROOT_PKG}/__init__.py'.")
        res = search_inside_top(cand)
        if res:
            return res
        die(f"В указанной папке '{provided_root}' не найдена '{ROOT_PKG}/__init__.py'.")

    script_dir = os.path.dirname(os.path.abspath(script_path))
    parent_of_script = os.path.abspath(os.path.join(script_dir, '..'))
    candidates = [parent_of_script, os.path.abspath(os.getcwd()), script_dir]

    for cand in candidates:
        if os.path.basename(cand).lower() == 'src' and _has_bioetl_at(cand):
            return os.path.dirname(cand)
        if _has_bioetl_at(cand):
            return cand

    cur = script_dir
    while True:
        src_candidate = os.path.join(cur, 'src')
        if _has_bioetl_at(cur):
            return _normalize_repo_candidate(cur)
        if os.path.isdir(src_candidate) and _has_bioetl_at(src_candidate):
            return cur
        parent = os.path.abspath(os.path.join(cur, '..'))
        if parent == cur:
            break
        cur = parent

    max_depth = 4
    base_depth = parent_of_script.count(os.sep)
    for root, dirs, files in os.walk(parent_of_script):
        depth = root.count(os.sep) - base_depth
        if depth > max_depth:
            dirs[:] = []
            continue
        if ROOT_PKG in dirs and os.path.isfile(os.path.join(root, ROOT_PKG, '__init__.py')):
            return _normalize_repo_candidate(root)

    base = os.path.abspath(os.getcwd())
    base_depth = base.count(os.sep)
    for root, dirs, files in os.walk(base):
        depth = root.count(os.sep) - base_depth
        if depth > max_depth:
            dirs[:] = []
            continue
        if ROOT_PKG in dirs and os.path.isfile(os.path.join(root, ROOT_PKG, '__init__.py')):
            return _normalize_repo_candidate(root)

    return None

def find_packages(root_pkg_path):
    pkgs = []
    for root, dirs, files in os.walk(root_pkg_path):
        if any(part.startswith('.') for part in root.split(os.sep)):
            continue
        if '__init__.py' in files:
            rel = os.path.relpath(root, start=root_pkg_path)
            if rel in ('.', ''):
                pkg = ROOT_PKG
            else:
                parts = rel.split(os.sep)
                if any(p.lower().startswith('test') or p.lower() == 'tests' for p in parts):
                    continue
                pkg = ROOT_PKG + '.' + '.'.join(parts)
            if 'test' in pkg.lower():
                continue
            pkgs.append(pkg)
    return sorted(set(pkgs))

def run_pyreverse(pkg, project_name, repo_root):
    env = os.environ.copy()
    env['PYTHONPATH'] = repo_root + (os.pathsep + env.get('PYTHONPATH', '')) if env.get('PYTHONPATH') else repo_root
    cmd = ['pyreverse', '-o', 'dot', '-p', project_name] + PYREVERSE_OPTS + [pkg]
    print("  pyreverse:", ' '.join(cmd))
    subprocess.run(cmd, check=True, env=env, cwd=repo_root)

def find_specific_dotfiles(repo_root, project_name):
    class_candidates = [
        os.path.join(repo_root, f'classes_{project_name}.dot'),
        os.path.join(repo_root, 'classes.dot'),
    ]
    package_candidates = [
        os.path.join(repo_root, f'packages_{project_name}.dot'),
        os.path.join(repo_root, 'packages.dot'),
    ]
    class_dot = next((c for c in class_candidates if os.path.exists(c)), None)
    if class_dot is None:
        for f in os.listdir(repo_root):
            if f.startswith('classes_') and f.endswith('.dot') and project_name in f:
                class_dot = os.path.join(repo_root, f)
                break

    package_dot = next((c for c in package_candidates if os.path.exists(c)), None)
    if package_dot is None:
        for f in os.listdir(repo_root):
            if f.startswith('packages_') and f.endswith('.dot') and project_name in f:
                package_dot = os.path.join(repo_root, f)
                break

    return class_dot, package_dot

def _pkg_short_name(pkg_full):
    """
    Возвращает 'pkg_short' — последняя часть полного имени пакета,
    очищённая для безопасного имени папки.
    """
    last = pkg_full.split('.')[-1]
    # позволяем буквы/цифры/подчеркивания/дефисы и точки, остальное заменяем на '_'
    safe = re.sub(r'[^0-9A-Za-z_.-]', '_', last)
    return safe or 'pkg'

def clean_class_dot(infile, outfile):
    with open(infile, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    node_decl_re = re.compile(r'^\s*"(?P<name>[^"]+)"\s*\[.*\];\s*$')
    remove_names = set()
    for line in lines:
        m = node_decl_re.match(line)
        if not m:
            continue
        fullname = m.group('name')
        simple = fullname.split('.')[-1]
        parts = fullname.split('.')
        low = fullname.lower()
        if simple.startswith('_') or any(p.startswith('test') or p == 'tests' for p in parts) or '.test.' in low or '.tests.' in low:
            remove_names.add(fullname)

    if remove_names:
        print(f"    Удаляю {len(remove_names)} узлов (приватные/тесты) из class-dot.")
    else:
        print("    Чистка class-dot не потребовалась.")

    with open(outfile, 'w', encoding='utf-8') as out:
        for line in lines:
            skip = False
            for rn in remove_names:
                if f'"{rn}"' in line:
                    skip = True
                    break
            if skip:
                continue
            out.write(line)

    with open(outfile, 'r', encoding='utf-8') as f:
        txt = f.read()
    if 'rankdir' not in txt:
        txt = re.sub(r'^(digraph\s+"[^"]+"\s*\{\s*)', r'\1\n    rankdir=%s;\n' % RANKDIR, txt, flags=re.MULTILINE)
        with open(outfile, 'w', encoding='utf-8') as f:
            f.write(txt)

def clean_package_dot(infile, outfile):
    with open(infile, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    node_decl_re = re.compile(r'^\s*"(?P<name>[^"]+)"\s*\[.*\];\s*$')
    remove_names = set()
    for line in lines:
        m = node_decl_re.match(line)
        if not m:
            continue
        fullname = m.group('name')
        parts = fullname.split('.')
        low = fullname.lower()
        if any(p.startswith('test') or p == 'tests' for p in parts) or '.test.' in low or '.tests.' in low:
            remove_names.add(fullname)

    if remove_names:
        print(f"    Удаляю {len(remove_names)} узлов (пакеты с test) из package-dot.")
    else:
        print("    Чистка package-dot не потребовалась.")

    with open(outfile, 'w', encoding='utf-8') as out:
        for line in lines:
            skip = False
            for rn in remove_names:
                if f'"{rn}"' in line:
                    skip = True
                    break
            if skip:
                continue
            out.write(line)

    with open(outfile, 'r', encoding='utf-8') as f:
        txt = f.read()
    if 'rankdir' not in txt:
        txt = re.sub(r'^(digraph\s+"[^"]+"\s*\{\s*)', r'\1\n    rankdir=%s;\n' % RANKDIR, txt, flags=re.MULTILINE)
        with open(outfile, 'w', encoding='utf-8') as f:
            f.write(txt)

def render_dot_to_files(dotfile, svg_out, png_out):
    print(f"    render -> {svg_out}, {png_out}")
    env = os.environ.copy()
    subprocess.run(['dot', '-Tsvg', f'-Gdpi={DPI}', f'-Grankdir={RANKDIR}', dotfile, '-o', svg_out], check=True, env=env)
    subprocess.run(['dot', '-Tpng', f'-Gdpi={DPI}', f'-Grankdir={RANKDIR}', dotfile, '-o', png_out], check=True, env=env)

def main():
    parser = argparse.ArgumentParser(description="Generate class and package diagrams for bioetl.* packages (per-package subfolders)")
    parser.add_argument('--repo-root', '-r', help="Explicit path to repository root (folder containing bioetl/). "
                                               "If you pass a 'src' folder, script will use its parent as repo root.")
    args = parser.parse_args()

    check_tools()

    script_path = os.path.abspath(__file__)
    repo_root = find_repo_root(script_path, provided_root=args.repo_root)
    if not repo_root:
        die("Не удалось найти корень репозитория с пакетом 'bioetl'.\n"
            "Попробуйте --repo-root \"E:\\\\github\\\\bioactivity_data_acquisition1\" или поместите скрипт в tools/ рядом с корнем репо.")
    print("Корень репозитория (где будут сохранены диаграммы):", repo_root)

    # determine bioetl_path: prefer repo_root/bioetl, else repo_root/src/bioetl
    bioetl_path = os.path.join(repo_root, ROOT_PKG)
    if not os.path.isdir(bioetl_path):
        alt = os.path.join(repo_root, 'src', ROOT_PKG)
        if os.path.isdir(alt):
            bioetl_path = alt

    print("Папка пакета:", bioetl_path)
    os.chdir(repo_root)

    if not os.path.isdir(bioetl_path):
        die(f"Папка {bioetl_path} не найдена (ожидается bioetl/ или src/bioetl)")

    pkgs = find_packages(bioetl_path)
    if not pkgs:
        die("Не найдено подпакетов внутри 'bioetl'.")

    outdir = os.path.join(repo_root, OUTDIR_NAME)
    dot_root = os.path.join(outdir, 'dot')
    svg_root = os.path.join(outdir, 'svg')
    png_root = os.path.join(outdir, 'png')
    os.makedirs(dot_root, exist_ok=True)
    os.makedirs(svg_root, exist_ok=True)
    os.makedirs(png_root, exist_ok=True)

    print(f"Найдено {len(pkgs)} пакетов. Генерирую диаграммы в {outdir} ...")
    for pkg in pkgs:
        print("Пакет:", pkg)
        project_name = pkg.replace('.', '_')
        pkg_short = _pkg_short_name(pkg)

        # prepare per-package subdirs
        dot_dir = os.path.join(dot_root, pkg_short)
        svg_dir = os.path.join(svg_root, pkg_short)
        png_dir = os.path.join(png_root, pkg_short)
        os.makedirs(dot_dir, exist_ok=True)
        os.makedirs(svg_dir, exist_ok=True)
        os.makedirs(png_dir, exist_ok=True)

        try:
            run_pyreverse(pkg, project_name, repo_root)
        except subprocess.CalledProcessError as e:
            print("  Ошибка pyreverse:", e)
            continue

        class_dot, package_dot = find_specific_dotfiles(repo_root, project_name)

        # === Process class dot ===
        if class_dot:
            try:
                class_copy = os.path.join(dot_dir, os.path.basename(class_dot))
                copy2(class_dot, class_copy)
            except Exception:
                class_copy = class_dot
            class_clean = os.path.join(dot_dir, f'{project_name}.classes.clean.dot')
            try:
                clean_class_dot(class_copy, class_clean)
                svg_out = os.path.join(svg_dir, f'{project_name}_classes.svg')
                png_out = os.path.join(png_dir, f'{project_name}_classes.png')
                render_dot_to_files(class_clean, svg_out, png_out)
            except Exception as e:
                print("  Ошибка при обработке class-dot:", e)
        else:
            print("  Class .dot не найден для пакета; пропускаю class-diagram.")

        # === Process package dot ===
        if package_dot:
            try:
                pkg_copy = os.path.join(dot_dir, os.path.basename(package_dot))
                copy2(package_dot, pkg_copy)
            except Exception:
                pkg_copy = package_dot
            pkg_clean = os.path.join(dot_dir, f'{project_name}.packages.clean.dot')
            try:
                clean_package_dot(pkg_copy, pkg_clean)
                svg_out = os.path.join(svg_dir, f'{project_name}_packages.svg')
                png_out = os.path.join(png_dir, f'{project_name}_packages.png')
                render_dot_to_files(pkg_clean, svg_out, png_out)
            except Exception as e:
                print("  Ошибка при обработке package-dot:", e)
        else:
            print("  Package .dot не найден для пакета; пропускаю package-diagram.")

    print("Генерация завершена. Файлы находятся в:", outdir)
    print("  - DOT root:", dot_root)
    print("  - SVG root:", svg_root)
    print("  - PNG root:", png_root)

if __name__ == '__main__':
    main()
