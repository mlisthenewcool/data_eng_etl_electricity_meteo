---
name: review_code
description: In-depth code review (conventions, architecture, logic, docs, performance, security).
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash(git diff *)
argument-hint: "<scope: all | uncommitted | module name | tests | notebooks | file paths>"
---

Review the code defined by `$ARGUMENTS` across six dimensions : project conventions,
architecture, logic, documentation, performance, and security.

## Steps

1. **Resolve scope to files** :
    - **all** : `src/**/*.py`
    - **uncommitted** : run `git diff HEAD --name-only -- '*.py'` (staged + unstaged)
    - **tests** : `tests/**/*.py`
    - **notebooks** : `notebooks/**/*.py`
    - **module name** (e.g. `core`, `pipeline`, `airflow`, or any future module) :
      glob `src/**/<name>/**/*.py` — works for any subdirectory under `src/`
    - **file paths** : read the given files directly
    - **no argument** : ask the user what to review

2. **Read every matching file in full.** Also read the key modules they import
   (exceptions, types, settings) to verify correct usage.

3. **Analyze each dimension** (details below).

4. **Provide structured feedback** :
    - **Summary** : Few lines on what the code does and overall assessment.
    - **Issues/suggestions found**, grouped by dimension then severity.
      For each finding, report :
        - `file:line`
        - One-line summary of the problem
        - Explain why it's a problem
        - Provide a concrete fix
        - Assign a severity : `[bloquant]` (bugs, security, broken contracts),
          `[important]` (conventions, design, performance), `[suggestion]` (
          improvements).

## Review dimensions

### 1. Project conventions (CLAUDE.md)

- Import style (absolute only, correct order)
- Type hints (modern syntax, mandatory on all signatures)
- Docstrings (numpy style, no type duplication)
- Error handling (domain exceptions from `core.exceptions`)
- Path resolution (PathResolver, no hardcoded paths)
- Naming & language conventions (code in English, commits in French)

### 2. Architecture & design

- Separation of concerns : core / utils / pipeline / airflow boundaries respected
- Coupling : modules depend only on their allowed neighbors; no circular imports
- Abstraction level : no business logic in utils, no orchestration in core
- Single responsibility : each class/module has one clear purpose
- Consistency : similar problems solved the same way across the codebase
- Over-engineering : unnecessary abstractions, premature generalization

### 3. Logic & correctness

- Edge cases : empty collections, None values, boundary conditions
- Error paths : exceptions caught at the right level, no silent swallowing
- Data integrity : schema mismatches, missing columns, type coercions
- State management : mutable shared state, race conditions in async code
- Control flow : unreachable code, redundant conditions, off-by-one errors

### 4. Documentation

- Public API docstrings : present, accurate, numpy-style
- Complex logic : inline comments explain *why*, not *what*
- Module-level docstrings : describe purpose and key exports
- Outdated docs : comments/docstrings that contradict the current code
- Missing context : non-obvious business rules without explanation

### 5. Performance

- I/O patterns : unnecessary reads/writes, missing batching, unbounded fetches
- Data processing : Polars antipatterns (row-by-row iteration, unnecessary collects)
- Memory : loading entire datasets when streaming/lazy would suffice
- Algorithmic : O(n²) where O(n) is possible, repeated expensive computations
- Resource management : unclosed connections/files, missing context managers

### 6. Security

- Injection : SQL/command injection via string formatting with user input
- Secrets : hardcoded credentials, tokens, API keys; secrets in logs
- Input validation : untrusted data (API responses, file contents) used without checks
- Path traversal : user-controlled paths without sanitization
- Dependencies : known vulnerable patterns (e.g. `eval`, `pickle.loads` on untrusted
  data)
- Permissions : overly broad file/network access

## Rules

- **Always recommend the best architectural choice** — prioritize code quality,
  software architecture, and data engineering best practices over minimizing the
  number of changes. Never settle for a "good enough" fix when a better design exists.
  The cost of refactoring is acceptable; the cost of technical debt is not.
- Skip dimensions and severity categories with zero findings.
- Don't flag what ruff/ty already catch (formatting, unused imports, basic type errors).
- If the code is good, say so. Don't invent issues.
- If the scope exceeds 50 files, give a per-module overview first and ask where to
  dive in.
- Prioritise bloquant findings : surface them first so they get fixed first.
