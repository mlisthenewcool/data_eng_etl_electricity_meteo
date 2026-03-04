"""Reflow multi-line docstrings to respect the 88-char doc-length limit.

Rules
-----
1. **Compaction** — consecutive prose lines in the same paragraph are joined then
   re-wrapped at *max_doc_length* (88 chars, indentation included).
2. **One-sentence-per-line** — after compaction, if splitting on sentence boundaries
   produces the same number of lines (or fewer), that version is preferred.

Elements left untouched: one-liners, bullet/numbered lists, numpy section headers and
separators, code blocks (``>>>`` / ``::``), blank lines, box-drawing trees, indented
code examples.
"""

import argparse
import ast
import difflib
import re
import sys
import textwrap
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_DOC_LENGTH = 88

# Sentence-ending punctuation followed by a space + uppercase letter.
# Negative lookbehind avoids splitting on common abbreviations.
_SENTENCE_SPLIT_RE = re.compile(
    r"(?<!\be\.g\.)"  # not after e.g.
    r"(?<!\bi\.e\.)"  # not after i.e.
    r"(?<!\betc\.)"  # not after etc.
    r"(?<!\bvs\.)"  # not after vs.
    r"(?<=[.!?])"  # after sentence-ending punctuation
    r"\s+"  # whitespace separator
    r"(?=[A-Z`*])"  # next sentence starts with uppercase, backtick, or emphasis
)

# Lines that must NOT be reflowed.
_SKIP_LINE_RE = re.compile(
    r"^\s*[-*]\s"  # bullet list
    r"|^\s*\d+[.)]\s"  # numbered list
    r"|^\s*>>>"  # doctest
    r"|^-{3,}$"  # numpy section separator
    r"|^\.\.\s"  # reST directive
    r"|[├└│┌┐┘┤┬┴┼╭╮╯╰─]"  # box-drawing characters (tree diagrams)
)

# Numpy-style section headers (word(s) followed by a line of dashes).
_SECTION_HEADER_RE = re.compile(r"^[A-Z][A-Za-z ]*$")
_SEPARATOR_RE = re.compile(r"^-{3,}$")

# Indented function call (code example heuristic).
_CODE_LINE_RE = re.compile(r"^\w+\s*\(")

# Numpy parameter/attribute entry with a trailing colon and no type annotation.
# Matches lines like "    param_name:" and captures everything before the colon.
_PARAM_COLON_RE = re.compile(r"^(\s+\w+):\s*$")

# Parenthesized groups — spaces inside are protected from line breaks.
_PAREN_GROUP_RE = re.compile(r"\([^)]*\)")

# Non-breaking space placeholder used during wrapping.
_NBSP = "\xa0"

# Docstring regex: captures opening quotes, body, closing quotes.
_DOCSTRING_RE = re.compile(
    r'(""")(.*?)(""")',
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# AST-based docstring classifier
# ---------------------------------------------------------------------------


def _get_docstring_line_ranges(source: str) -> list[tuple[int, int]]:
    """Return ``(start_line, end_line)`` 1-indexed for every real docstring.

    Uses AST to identify only real docstrings (module, class, function), not
    arbitrary triple-quoted strings (SQL queries, templates, etc.).
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    ranges: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        ds_node = _get_docstring_node(node)
        if ds_node is None or ds_node.end_lineno is None:
            continue
        ranges.append((ds_node.lineno, ds_node.end_lineno))

    ranges.sort()
    return ranges


def _get_docstring_node(node: ast.AST) -> ast.Constant | None:
    """Return the AST node for the docstring of *node*, or ``None``."""
    if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
        body = node.body
    else:
        return None

    if not body:
        return None

    first_stmt = body[0]
    if (
        isinstance(first_stmt, ast.Expr)
        and isinstance(first_stmt.value, ast.Constant)
        and isinstance(first_stmt.value.value, str)
    ):
        return first_stmt.value
    return None


def _line_number_at_offset(source: str, offset: int) -> int:
    """Return the 1-indexed line number at character *offset* in *source*."""
    return source[:offset].count("\n") + 1


# ---------------------------------------------------------------------------
# Text reflow helpers
# ---------------------------------------------------------------------------


def _is_skip_line(line: str) -> bool:
    """Return ``True`` if *line* should not be reflowed."""
    return bool(_SKIP_LINE_RE.search(line))


def _is_section_separator(line: str) -> bool:
    """Return ``True`` if *line* is a numpy-style ``----------`` separator."""
    return bool(_SEPARATOR_RE.match(line.strip()))


def _is_section_header(line: str, next_line: str | None) -> bool:
    """Return ``True`` if *line* is a numpy section header (followed by dashes)."""
    if next_line is None:
        return False
    return bool(_SECTION_HEADER_RE.match(line.strip()) and _SEPARATOR_RE.match(next_line.strip()))


def _is_code_block_marker(line: str) -> bool:
    """Return ``True`` if *line* ends with ``::`` (reST code block opener)."""
    return line.rstrip().endswith("::")


def _looks_like_code_block(lines: list[str]) -> bool:
    """Return ``True`` if all lines in *lines* look like indented code examples."""
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if _CODE_LINE_RE.match(stripped):
            continue
        return False
    return len(lines) > 0


def _protect_parens(text: str, max_group_len: int) -> str:
    """Replace spaces inside short ``(...)`` groups with non-breaking spaces.

    Groups longer than *max_group_len* are left breakable — they cannot fit on
    a single line anyway.
    """

    def _protect_if_short(m: re.Match[str]) -> str:
        group = m.group(0)
        if len(group) > max_group_len:
            return group
        return group.replace(" ", _NBSP)

    return _PAREN_GROUP_RE.sub(_protect_if_short, text)


def _fill(text: str, width: int) -> str:
    """Wrap *text* with parenthesis-aware protection."""
    protected = _protect_parens(text, max_group_len=width)
    wrapped = textwrap.fill(protected, width=width, break_on_hyphens=False)
    return wrapped.replace(_NBSP, " ")


def _wrap_text(text: str, width: int, indent: str) -> list[str]:
    """Wrap *text* to *width* chars (including *indent* prefix)."""
    available = max(width - len(indent), 20)
    wrapped = _fill(text, available)
    return [f"{indent}{ln}" for ln in wrapped.splitlines()]


def _wrap_sentences(text: str, width: int, indent: str) -> list[str]:
    """Split *text* on sentence boundaries, then wrap each sentence."""
    sentences = _SENTENCE_SPLIT_RE.split(text)
    result: list[str] = []
    available = max(width - len(indent), 20)
    for raw_sentence in sentences:
        stripped = raw_sentence.strip()
        if not stripped:
            continue
        wrapped = _fill(stripped, available)
        result.extend(f"{indent}{ln}" for ln in wrapped.splitlines())
    return result


# ---------------------------------------------------------------------------
# Paragraph grouping
# ---------------------------------------------------------------------------


def _detect_indent(line: str) -> str:
    """Return the leading whitespace of *line*."""
    return line[: len(line) - len(line.lstrip())]


def _reflow_paragraph(lines: list[str], max_width: int) -> list[str]:
    """Reflow a paragraph (list of consecutive prose lines).

    Applies compaction then one-sentence-per-line conditionally.
    Preserves indented code example blocks unchanged.
    """
    if not lines or _looks_like_code_block(lines):
        return lines

    indent = _detect_indent(lines[0])
    joined = " ".join(ln.strip() for ln in lines)

    compact = _wrap_text(joined, max_width, indent)
    sentence = _wrap_sentences(joined, max_width, indent)

    n_orig = len(lines)
    n_compact = len(compact)
    n_sentence = len(sentence)
    has_multiple_sentences = sentence != compact

    # Compaction saves lines → apply, prefer sentence split if same or fewer
    if n_compact < n_orig:
        return sentence if n_sentence <= n_compact else compact

    # Same line count — prefer sentence split if multiple sentences
    if has_multiple_sentences and n_sentence <= n_orig:
        return sentence

    # Same line count, single sentence — apply compact only if break points
    # differ (fixes parenthesized groups split across lines)
    orig_normalized = [ln.strip() for ln in lines]
    compact_normalized = [ln.strip() for ln in compact]
    if n_compact == n_orig and compact_normalized != orig_normalized:
        return compact

    return lines


def _flush_paragraph(paragraph: list[str], result: list[str], max_width: int) -> None:
    """Reflow *paragraph* into *result* and clear *paragraph* in-place."""
    if paragraph:
        result.extend(_reflow_paragraph(paragraph, max_width))
        paragraph.clear()


def _handle_code_block(body_lines: list[str], i: int, result: list[str]) -> int:
    """Consume lines belonging to a reST code block, appending them to *result*.

    Returns the index of the first line after the code block.
    """
    marker_indent = len(_detect_indent(body_lines[i - 1])) if i > 0 else 0
    seen_content = False

    while i < len(body_lines):
        line = body_lines[i]
        stripped = line.strip()

        if stripped == "":
            result.append(line)
            i += 1
            continue

        line_indent = len(_detect_indent(line))
        if seen_content and line_indent <= marker_indent:
            break

        seen_content = True
        result.append(line)
        i += 1

    return i


def _reflow_docstring_body(body_lines: list[str], max_width: int) -> list[str]:
    """Reflow all prose paragraphs in a docstring body.

    Preserves blank lines, section headers/separators, lists, code blocks, and
    definition-list entries (indented descriptions under parameter names).
    """
    result: list[str] = []
    paragraph: list[str] = []
    in_code_block = False
    i = 0

    while i < len(body_lines):
        line = body_lines[i]

        # Strip trailing colon from numpy parameter/attribute entries (no type).
        line = _PARAM_COLON_RE.sub(r"\1", line)

        stripped = line.strip()

        if _is_code_block_marker(line) and not in_code_block:
            _flush_paragraph(paragraph, result, max_width)
            result.append(line)
            in_code_block = True
            i += 1
            continue

        if in_code_block:
            i = _handle_code_block(body_lines, i, result)
            in_code_block = False
            continue

        if stripped == "":
            _flush_paragraph(paragraph, result, max_width)
            result.append(line)
            i += 1
            continue

        next_line = body_lines[i + 1] if i + 1 < len(body_lines) else None
        if (
            _is_section_header(line, next_line)
            or _is_section_separator(line)
            or _is_skip_line(line)
        ):
            _flush_paragraph(paragraph, result, max_width)
            result.append(line)
            i += 1
            continue

        if paragraph and _detect_indent(paragraph[0]) != _detect_indent(line):
            _flush_paragraph(paragraph, result, max_width)

        paragraph.append(line)
        i += 1

    _flush_paragraph(paragraph, result, max_width)
    return result


# ---------------------------------------------------------------------------
# Docstring processing
# ---------------------------------------------------------------------------


def _process_docstring(match: re.Match[str]) -> str:
    """Process a single docstring match and return the reformatted version."""
    open_q = match.group(1)
    body = match.group(2)
    close_q = match.group(3)

    if "\n" not in body:
        return match.group(0)

    lines = body.split("\n")
    summary = lines[0]
    rest = lines[1:]

    reflowed = _reflow_docstring_body(rest, MAX_DOC_LENGTH)
    return open_q + summary + "\n" + "\n".join(reflowed) + close_q


def _find_and_reflow_docstrings(source: str) -> str:
    """Find real docstrings via AST, reflow them via regex."""
    docstring_ranges = _get_docstring_line_ranges(source)
    if not docstring_ranges:
        return source

    docstring_lines: set[int] = set()
    for start_line, end_line in docstring_ranges:
        docstring_lines.update(range(start_line, end_line + 1))

    result_parts: list[str] = []
    last_end = 0

    for match in _DOCSTRING_RE.finditer(source):
        match_line = _line_number_at_offset(source, match.start())
        if match_line not in docstring_lines:
            continue

        result_parts.append(source[last_end : match.start()])
        result_parts.append(_process_docstring(match))
        last_end = match.end()

    result_parts.append(source[last_end:])
    return "".join(result_parts)


def format_file(path: Path) -> tuple[str, str]:
    """Read *path*, reflow docstrings, return ``(original, formatted)``."""
    original = path.read_text(encoding="utf-8")
    formatted = _find_and_reflow_docstrings(original)
    return original, formatted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    """Entry point: parse args, reflow docstrings, report results."""
    parser = argparse.ArgumentParser(description="Reflow Python docstrings to 88 chars")
    parser.add_argument("paths", nargs="+", type=Path, help="Files or directories to process")
    parser.add_argument("--check", action="store_true", help="Exit 1 if files would change")
    parser.add_argument("--diff", action="store_true", help="Print unified diff")
    args = parser.parse_args()

    files: list[Path] = []
    for p in args.paths:
        if p.is_file():
            files.append(p)
        elif p.is_dir():
            files.extend(sorted(p.rglob("*.py")))

    changed = 0
    for fpath in files:
        original, formatted = format_file(fpath)
        if original == formatted:
            continue
        changed += 1

        if args.diff:
            diff = difflib.unified_diff(
                original.splitlines(keepends=True),
                formatted.splitlines(keepends=True),
                fromfile=str(fpath),
                tofile=str(fpath),
            )
            sys.stdout.writelines(diff)
        elif not args.check:
            fpath.write_text(formatted, encoding="utf-8")
            print(f"Reformatted {fpath}")

    if args.check:
        if changed:
            print(f"{changed} file(s) would be reformatted")
            return 1
        print("All files OK")
        return 0

    if changed:
        print(f"\n{changed} file(s) reformatted")
    else:
        print("No changes needed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
