#!/usr/bin/env python3
"""
analyze_src.py

Usage:
    python3 tools/analyze_src.py --src src --tests tests > full_report.md

Output: Markdown report with
 - таблица функций/методов
 - таблица групп дубликатов
 - таблица кандидатов на мёртвый код
"""
import ast
import os
import sys
import argparse
import hashlib
import tokenize
import io
from collections import defaultdict
from typing import Set

# ---------- normalisers ----------
class NormaliseNames(ast.NodeTransformer):
    def visit_Name(self, node: ast.Name):
        return ast.copy_location(ast.Name(id="__VAR__", ctx=node.ctx), node)
    def visit_Attribute(self, node: ast.Attribute):
        self.generic_visit(node)
        node.attr = "__ATTR__"
        return node
    def visit_arg(self, node: ast.arg):
        annotation = node.annotation
        new_node = ast.arg(arg="__ARG__", annotation=annotation, type_comment=None)
        return ast.copy_location(new_node, node)
    def visit_FunctionDef(self, node: ast.FunctionDef):
        new = self.generic_visit(node)
        new.name = "__FUNC__"
        return new
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        new = self.generic_visit(node)
        new.name = "__FUNC__"
        return new
    def visit_Constant(self, node: ast.Constant):
        if isinstance(node.value, (int, float, complex, str, bytes)):
            return ast.copy_location(ast.Constant(value=type(node.value)()), node)
        return node

def ast_hash_of_node(node: ast.AST) -> str:
    tree = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    normalizer = NormaliseNames()
    normal = normalizer.visit(tree)
    dump = ast.dump(normal, include_attributes=False)
    return hashlib.sha256(dump.encode('utf-8')).hexdigest()

def tokenize_source_segment(src: str) -> Set[str]:
    tokens = set()
    reader = io.StringIO(src).readline
    try:
        for tok in tokenize.generate_tokens(reader):
            if tok.type in (tokenize.ENCODING, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER, tokenize.COMMENT):
                continue
            lex = tok.string.strip()
            if lex:
                tokens.add(lex)
    except Exception:
        pass
    return tokens

# ---------- def collector ----------
class DefInfo:
    def __init__(self, qualname, kind, file_rel, lineno, end_lineno, body_len, complexity, signature, ast_hash, tokens):
        self.qualname = qualname
        self.kind = kind
        self.file = file_rel
        self.lineno = lineno
        self.end_lineno = end_lineno
        self.body_len = body_len
        self.complexity = complexity
        self.signature = signature
        self.ast_hash = ast_hash
        self.tokens = tokens
        self.call_count = 0
        self.call_count_in_tests = 0

def format_sig(node: ast.FunctionDef) -> str:
    args = node.args
    parts = []
    def render_arg(a, default=None):
        s = a.arg
        if a.annotation:
            try:
                s += f":{ast.unparse(a.annotation)}"
            except Exception:
                pass
        if default is not None:
            try:
                s += f"={ast.unparse(default)}"
            except Exception:
                s += "=..."
        return s
    pos = list(args.args)
    defaults = list(args.defaults)
    offset = len(pos) - len(defaults)
    for i, a in enumerate(pos):
        d = defaults[i-offset] if i >= offset else None
        parts.append(render_arg(a, d))
    if args.vararg:
        parts.append("*" + args.vararg.arg)
    for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=False):
        parts.append(render_arg(a, d))
    if args.kwarg:
        parts.append("**" + args.kwarg.arg)
    ret = ""
    if node.returns:
        try:
            ret = "->" + ast.unparse(node.returns)
        except Exception:
            ret = "->..."
    return f"{node.name}({', '.join(parts)}){ret}"

class DefCollector(ast.NodeVisitor):
    def __init__(self, file_rel, source_lines):
        self.file_rel = file_rel
        self.source_lines = source_lines
        self.class_stack = []
        self.defs = []

    def visit_ClassDef(self, node: ast.ClassDef):
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        kind = 'method' if self.class_stack else 'function'
        qual = ".".join([*self.class_stack, node.name]) if self.class_stack else node.name
        end = getattr(node, 'end_lineno', None) or node.lineno
        body_len = max(0, end - node.lineno)
        complexity = 0
        for n in ast.walk(node):
            if isinstance(n, (ast.If, ast.For, ast.While, ast.Try, ast.With, ast.AsyncFor, ast.AsyncWith)):
                complexity += 1
        signature = format_sig(node)
        ah = ast_hash_of_node(node)
        src_segment = ast.get_source_segment("\n".join(self.source_lines), node) or ""
        tokens = tokenize_source_segment(src_segment)
        di = DefInfo(qual, kind, self.file_rel, node.lineno, end, body_len, complexity, signature, ah, tokens)
        self.defs.append(di)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self.visit_FunctionDef(node)

# ---------- call collector ----------
def extract_call_names_from_node(node: ast.AST):
    names = []
    if isinstance(node, ast.Call):
        f = node.func
        if isinstance(f, ast.Name):
            names.append(f.id)
        elif isinstance(f, ast.Attribute):
            names.append(f.attr)
            if isinstance(f.value, ast.Name):
                names.append(f"{f.value.id}.{f.attr}")
    return names

class CallCollector(ast.NodeVisitor):
    def __init__(self):
        self.calls = []
    def visit_Call(self, node):
        names = extract_call_names_from_node(node)
        if names:
            self.calls.append((getattr(node, 'lineno', None), names))
        self.generic_visit(node)

# ---------- helpers ----------
def iter_py_files(root):
    for dirpath, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for fn in files:
            if fn.endswith(".py"):
                yield os.path.join(dirpath, fn)

def relpath(base, path):
    return os.path.relpath(path, base).replace("\\\\", "/")

# ---------- main analysis ----------
def analyze(src_root, tests_root=None):
    defs_by_file = {}
    all_defs = []
    for fpath in iter_py_files(src_root):
        with open(fpath, 'r', encoding='utf-8') as f:
            src_txt = f.read()
        lines = src_txt.splitlines()
        try:
            tree = ast.parse(src_txt)
        except Exception as exc:
            print(f"# WARN: parse failed for {fpath}: {exc}", file=sys.stderr)
            continue
        dc = DefCollector(relpath(src_root, fpath), lines)
        dc.visit(tree)
        if dc.defs:
            defs_by_file[dc.file_rel] = dc.defs
            all_defs.extend(dc.defs)

    defs_by_name = defaultdict(list)
    for d in all_defs:
        parts = d.qualname.split(".")
        simple = parts[-1]
        defs_by_name[simple].append(d)
        if d.kind == 'method' and '.' in d.qualname:
            defs_by_name[d.qualname].append(d)

    call_locations = defaultdict(list)
    tests_call_locations = defaultdict(list)
    for fpath in iter_py_files(src_root):
        with open(fpath, 'r', encoding='utf-8') as f:
            src_txt = f.read()
        try:
            tree = ast.parse(src_txt)
        except Exception:
            continue
        cc = CallCollector()
        cc.visit(tree)
        for lineno, names in cc.calls:
            for n in names:
                call_locations[n].append((relpath(src_root, fpath), lineno))

    if tests_root and os.path.isdir(tests_root):
        for fpath in iter_py_files(tests_root):
            with open(fpath, 'r', encoding='utf-8') as f:
                src_txt = f.read()
            try:
                tree = ast.parse(src_txt)
            except Exception:
                continue
            cc = CallCollector()
            cc.visit(tree)
            for lineno, names in cc.calls:
                for n in names:
                    tests_call_locations[n].append((relpath(tests_root, fpath), lineno))

    for name, defs in defs_by_name.items():
        cnt = 0
        cnt_tests = 0
        if name in call_locations:
            cnt += len(call_locations[name])
        for full in list(call_locations.keys()):
            if '.' in full and full.endswith('.' + name):
                cnt += len(call_locations[full])
        if name in tests_call_locations:
            cnt_tests += len(tests_call_locations[name])
        for full in list(tests_call_locations.keys()):
            if '.' in full and full.endswith('.' + name):
                cnt_tests += len(tests_call_locations[full])
        if defs:
            per = cnt // len(defs)
            rem = cnt % len(defs)
            for i, d in enumerate(defs):
                d.call_count += per + (1 if i < rem else 0)
            per_t = cnt_tests // len(defs) if defs else 0
            rem_t = cnt_tests % len(defs)
            for i, d in enumerate(defs):
                d.call_count_in_tests += per_t + (1 if i < rem_t else 0)

    hash_buckets = defaultdict(list)
    for d in all_defs:
        hash_buckets[d.ast_hash].append(d)
    duplicate_groups = [bucket for bucket in hash_buckets.values() if len(bucket) > 1]

    near_pairs = []
    defs_list = all_defs
    def jaccard(a, b):
        if not a and not b:
            return 1.0
        i = len(a & b)
        u = len(a | b)
        return i / u if u else 0.0
    for i in range(len(defs_list)):
        for j in range(i+1, len(defs_list)):
            a = defs_list[i]
            b = defs_list[j]
            score = jaccard(a.tokens, b.tokens)
            if score >= 0.85 and a.ast_hash != b.ast_hash:
                near_pairs.append((a, b, score))

    lines = []
    lines.append("## Таблица функций/методов (полный каталог)\\n")
    lines.append("| Полное имя | Тип | Файл (относительно `src/`) | Верхнеуровневая директория | # строк | # прямых вызовов в `src/` | Статус использования |")
    lines.append("|---|---:|---|---|---:|---:|---|")
    for d in sorted(all_defs, key=lambda x: (x.file, x.lineno)):
        top_dir = d.file.split("/")[0] if "/" in d.file else d.file
        calls = d.call_count
        status = "не найдено вызовов"
        if calls > 0:
            status = "используется"
        if d.call_count_in_tests > 0 and calls == 0:
            status = "только tests"
        lines.append(f"| `{d.qualname}` | {d.kind} | `{d.file}` | `{top_dir}` | {d.body_len} | {calls} | {status} |")

    lines.append("\\n## Таблица дубликатов\\n")
    lines.append("| Группа | Функции/методы (полные имена) | Комментарий | Рекомендованный кандидат на источник |")
    lines.append("|---|---|---|---|")
    grp_id = 1
    for bucket in duplicate_groups:
        names = ", ".join(f"`{x.qualname}` (`{x.file}`)" for x in bucket)
        comment = "явный дубликат (идентичный нормализованный AST)"
        source = max(bucket, key=lambda x: x.call_count)
        lines.append(f"| G{grp_id} | {names} | {comment} | `{source.qualname}` (`{source.file}`) |")
        grp_id += 1

    if near_pairs:
        lines.append("\\n### Near-duplicates (по Jaccard токенов >=0.85 и разный AST)\\n")
        lines.append("| Пара | score | Обе реализации | Комментарий |")
        lines.append("|---|---:|---|---|")
        for a,b,score in near_pairs:
            comm = "структура похожа (большая доля общих токенов), но AST отличается"
            lines.append(f"| `{a.qualname}` ↔ `{b.qualname}` | {score:.2f} | `{a.file}` ↔ `{b.file}` | {comm} |")

    lines.append("\\n## Кандидаты на мёртвый код\\n")
    lines.append("| Объект (полное имя) | Файл | Причина | Уровень уверенности |")
    lines.append("|---|---|---|---:|")
    for d in all_defs:
        if d.call_count == 0 and d.call_count_in_tests == 0:
            level = "высокий"
            lines.append(f"| `{d.qualname}` | `{d.file}` | не найдено вызовов в src/ | {level} |")

    print("\\n".join(lines))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="src")
    parser.add_argument("--tests", default="tests")
    args = parser.parse_args()
    analyze(args.src, args.tests)
