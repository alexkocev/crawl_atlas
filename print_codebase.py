#!/usr/bin/env python3
"""
Print all project code to printed_codebase.txt, excluding venv, requirements, json, etc.
"""

from pathlib import Path

# Directory patterns to exclude (any path containing these)
EXCLUDE_DIRS = {
    "venv",
    "__pycache__",
    ".git",
    ".venv",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
}

# File patterns to exclude
EXCLUDE_FILES = {
    "requirements.txt",
    "requirements-dev.txt",
    "requirements.in",
    "printed_codebase.txt",  # Don't include the output itself
    "main_ecom.py",
    "main_healthdirect.py",
    "exploration_printed_content.txt",
    "exploration_print_content.py",
}

# File extensions to exclude
EXCLUDE_EXTENSIONS = {".json", ".pyc", ".pyo", ".egg-info", ".dist-info"}

# File extensions to include (code/source files). None = include all text files if not excluded
INCLUDE_EXTENSIONS = {
    ".py",
    ".md",
    ".yaml",
    ".yml",
    ".toml",
    ".cfg",
    ".ini",
    ".sh",
    ".sql",
    ".html",
    ".css",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".vue",
}


def should_include(path: Path, root: Path) -> bool:
    """Check if a file should be included in the output."""
    rel_path = path.relative_to(root)

    # Exclude if any part of path is in exclude dirs
    for part in rel_path.parts:
        if part in EXCLUDE_DIRS:
            return False

    # Exclude specific files
    if path.name in EXCLUDE_FILES:
        return False

    # Exclude by extension
    if path.suffix in EXCLUDE_EXTENSIONS:
        return False

    # Include only known code extensions
    if path.suffix in INCLUDE_EXTENSIONS:
        return True

    return False


def main():
    root = Path(__file__).parent.resolve()
    output_path = root / "printed_codebase.txt"

    lines = []
    lines.append("=" * 80)
    lines.append(f"CODEBASE: {root.name}")
    lines.append("=" * 80)

    # Collect and sort files
    files_to_include = []
    for path in root.rglob("*"):
        if path.is_file() and should_include(path, root):
            files_to_include.append(path.relative_to(root))

    files_to_include.sort()

    for rel_path in files_to_include:
        full_path = root / rel_path
        try:
            content = full_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            content = f"# Error reading file: {e}\n"

        lines.append("")
        lines.append("")
        lines.append("#" * 80)
        lines.append(f"# {rel_path}")
        lines.append("#" * 80)
        lines.append("")
        lines.append(content)

    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Written {len(files_to_include)} files to {output_path}")


if __name__ == "__main__":
    main()
