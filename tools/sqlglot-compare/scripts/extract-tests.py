#!/usr/bin/env python3
"""
Extract test cases from sqlglot's Python test files.

Parses Python AST to extract:
- validate_identity(sql, write_sql=None) calls
- validate_all(sql, read={}, write={}) calls

Outputs JSON files for each dialect in fixtures/extracted/dialects/
"""

import ast
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def get_string_value(node: ast.AST) -> Optional[str]:
    """Extract string value from an AST node."""
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    # Handle Python 3.7 compatibility (ast.Str deprecated in 3.8, removed in 3.12)
    if hasattr(ast, 'Str') and isinstance(node, ast.Str):
        return node.s
    if isinstance(node, ast.JoinedStr):
        # f-string - try to reconstruct
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif hasattr(ast, 'Str') and isinstance(value, ast.Str):
                parts.append(value.s)
            else:
                # Variable interpolation - skip
                return None
        return ''.join(parts)
    return None


def get_dict_value(node: ast.AST) -> Optional[Dict[str, str]]:
    """Extract dictionary from an AST node."""
    if not isinstance(node, ast.Dict):
        return None

    result = {}
    for key, value in zip(node.keys, node.values):
        key_str = get_string_value(key) if key else None
        val_str = get_string_value(value)
        if key_str and val_str:
            result[key_str] = val_str
    return result


def extract_validate_identity(call: ast.Call) -> Optional[Dict[str, Any]]:
    """Extract test case from validate_identity call."""
    if len(call.args) < 1:
        return None

    sql = get_string_value(call.args[0])
    if not sql:
        return None

    # Check for write_sql keyword argument
    expected = None
    identify = False
    for keyword in call.keywords:
        if keyword.arg == 'write_sql':
            expected = get_string_value(keyword.value)
        elif keyword.arg == 'identify':
            # Check if identify=True
            if isinstance(keyword.value, ast.Constant):
                identify = keyword.value.value == True
            elif isinstance(keyword.value, ast.NameConstant):  # Python 3.7
                identify = keyword.value.value == True

    # Also check second positional arg
    if len(call.args) >= 2 and expected is None:
        expected = get_string_value(call.args[1])

    result = {
        'sql': sql,
        'expected': expected,
    }

    # Only include identify if True (to keep JSON compact)
    if identify:
        result['identify'] = True

    return result


def extract_validate_all(call: ast.Call) -> Optional[Dict[str, Any]]:
    """Extract test case from validate_all call."""
    if len(call.args) < 1:
        return None

    sql = get_string_value(call.args[0])
    if not sql:
        return None

    read = {}
    write = {}

    for keyword in call.keywords:
        if keyword.arg == 'read':
            read = get_dict_value(keyword.value) or {}
        elif keyword.arg == 'write':
            write = get_dict_value(keyword.value) or {}

    # Skip if no read/write dicts
    if not read and not write:
        return None

    return {
        'sql': sql,
        'read': read,
        'write': write,
    }


def find_dialect_name(tree: ast.AST) -> Optional[str]:
    """Find the dialect name from a test class."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if isinstance(target, ast.Name) and target.id == 'dialect':
                            return get_string_value(item.value)
    return None


def extract_pretty_tests(filepath: str) -> List[Dict[str, Any]]:
    """
    Extract pretty-print test cases from pretty.sql file.
    Format: pairs of input/output separated by blank lines.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    tests = []
    blocks = content.split('\n\n')
    line_number = 1

    for block in blocks:
        trimmed = block.strip()
        if not trimmed:
            line_number += block.count('\n') + 1
            continue

        # Filter out comment lines starting with #
        lines = [l for l in trimmed.split('\n') if not l.startswith('#')]

        if len(lines) < 2:
            line_number += block.count('\n') + 1
            continue

        # Find where the input ends (first semicolon at paren depth 0)
        input_end = 0
        paren_depth = 0
        found_semicolon = False

        for i, line in enumerate(lines):
            for c in line:
                if c == '(':
                    paren_depth += 1
                elif c == ')':
                    paren_depth = max(0, paren_depth - 1)
                elif c == ';' and paren_depth == 0:
                    found_semicolon = True
            if found_semicolon and paren_depth == 0:
                input_end = i
                break

        # Split into input and expected
        if found_semicolon and input_end < len(lines) - 1:
            input_sql = '\n'.join(lines[:input_end + 1]).strip()
            expected = '\n'.join(lines[input_end + 1:]).strip()

            if input_sql and expected:
                tests.append({
                    'line': line_number,
                    'input': input_sql,
                    'expected': expected,
                })
        elif len(lines) >= 2:
            # Fallback: first line is input, rest is output
            input_sql = lines[0].strip()
            expected = '\n'.join(lines[1:]).strip()

            if input_sql and expected:
                tests.append({
                    'line': line_number,
                    'input': input_sql,
                    'expected': expected,
                })

        line_number += block.count('\n') + 1

    return tests


def extract_tests_from_file(filepath: str) -> Dict[str, Any]:
    """Extract all tests from a Python test file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"  Syntax error in {filepath}: {e}", file=sys.stderr)
        return {'dialect': None, 'identity': [], 'transpilation': []}

    dialect = find_dialect_name(tree)
    identity_tests = []
    transpilation_tests = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        # Check if it's a method call on self
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr

            if method_name == 'validate_identity':
                test = extract_validate_identity(node)
                if test:
                    identity_tests.append(test)

            elif method_name == 'validate_all':
                test = extract_validate_all(node)
                if test:
                    transpilation_tests.append(test)

    return {
        'dialect': dialect,
        'identity': identity_tests,
        'transpilation': transpilation_tests,
    }


def main():
    # Determine paths relative to script location
    script_dir = Path(__file__).parent.parent
    project_root = script_dir.parent.parent

    sqlglot_tests_dir = project_root / 'external-projects' / 'sqlglot' / 'tests'
    dialects_dir = sqlglot_tests_dir / 'dialects'
    fixtures_dir = sqlglot_tests_dir / 'fixtures'

    output_dir = script_dir / 'fixtures' / 'extracted' / 'dialects'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Also create identity.json and pretty.json in parent dir
    extracted_dir = script_dir / 'fixtures' / 'extracted'

    print("SQLGlot Test Extractor")
    print("=" * 60)

    # Extract identity tests
    identity_sql = fixtures_dir / 'identity.sql'
    if identity_sql.exists():
        with open(identity_sql, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#') and not line.strip().startswith('--')]

        identity_tests = [{'line': i + 1, 'sql': sql} for i, sql in enumerate(lines)]

        with open(extracted_dir / 'identity.json', 'w') as f:
            json.dump({'tests': identity_tests, 'count': len(identity_tests)}, f, indent=2)

        print(f"Identity tests: {len(identity_tests)} extracted")

    # Extract pretty tests
    pretty_sql = fixtures_dir / 'pretty.sql'
    if pretty_sql.exists():
        pretty_tests = extract_pretty_tests(str(pretty_sql))

        with open(extracted_dir / 'pretty.json', 'w') as f:
            json.dump({'tests': pretty_tests, 'count': len(pretty_tests)}, f, indent=2)

        print(f"Pretty tests: {len(pretty_tests)} extracted")

    print()
    print("Dialect tests:")
    print("-" * 60)

    # Find all dialect test files
    test_files = sorted(dialects_dir.glob('test_*.py'))

    total_identity = 0
    total_transpilation = 0

    for test_file in test_files:
        dialect_name = test_file.stem.replace('test_', '')

        # Skip non-dialect files
        if dialect_name in ('dialect', '__init__'):
            continue

        print(f"  {dialect_name}...", end=' ')

        tests = extract_tests_from_file(str(test_file))

        # Use filename-based dialect if not found in file
        if not tests['dialect']:
            tests['dialect'] = dialect_name

        # Write to JSON
        output_file = output_dir / f"{dialect_name}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tests, f, indent=2, ensure_ascii=False)

        identity_count = len(tests['identity'])
        transpile_count = len(tests['transpilation'])
        total_identity += identity_count
        total_transpilation += transpile_count

        print(f"{identity_count} identity, {transpile_count} transpilation")

    print("-" * 60)
    print(f"Total: {total_identity} identity tests, {total_transpilation} transpilation tests")
    print()
    print(f"Output written to: {output_dir}")


if __name__ == '__main__':
    main()
