#!/usr/bin/env python3
"""
Extract test cases from sqlglot's Python test files.

Parses Python AST to extract:
- validate_identity(sql, write_sql=None) calls
- validate_all(sql, read={}, write={}) calls
- self.validate(sql, target, **kwargs) calls from test_transpile.py
- assertEqual(parse_one(sql).sql(), expected) from test_parser.py
- assertRaises(ParseError) from test_parser.py

Outputs JSON files for each dialect in fixtures/extracted/dialects/
Plus transpile.json and parser.json in fixtures/extracted/
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


def get_bool_value(node: ast.AST) -> Optional[bool]:
    """Extract boolean value from an AST node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return node.value
    if hasattr(ast, 'NameConstant') and isinstance(node, ast.NameConstant):
        return node.value
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


def extract_transpile_tests(filepath: str) -> Dict[str, Any]:
    """
    Extract test cases from test_transpile.py.

    Extracts:
    - self.validate(sql, target, **kwargs) calls
    - assertEqual(transpile(sql, **kwargs)[0], target) calls

    Categorizes into:
    - normalization: no read/write dialect (parse generic, generate generic)
    - transpilation: has write= dialect (parse generic, generate with dialect)

    Skips:
    - pretty=True
    - leading_comma=True
    - identity=False
    - error_level tests
    - read= only (read dialect with no write)
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"  Syntax error in {filepath}: {e}", file=sys.stderr)
        return {'normalization': [], 'transpilation': []}

    normalization_tests = []
    transpilation_tests = []
    skipped = 0

    # Walk the AST looking for validate calls and assertEqual(transpile()) calls
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        if not isinstance(node.func, ast.Attribute):
            continue

        method_name = node.func.attr

        if method_name == 'validate':
            # self.validate(sql, target, **kwargs)
            if len(node.args) < 2:
                continue

            sql = get_string_value(node.args[0])
            target = get_string_value(node.args[1])
            if not sql or not target:
                continue

            # Parse keyword args
            kwargs = {}
            for kw in node.keywords:
                if kw.arg:
                    kwargs[kw.arg] = kw.value

            # Skip tests with pretty=True, leading_comma=True, identity=False
            if 'pretty' in kwargs:
                val = get_bool_value(kwargs['pretty'])
                if val is True:
                    skipped += 1
                    continue

            if 'leading_comma' in kwargs:
                val = get_bool_value(kwargs['leading_comma'])
                if val is True:
                    skipped += 1
                    continue

            if 'identity' in kwargs:
                val = get_bool_value(kwargs['identity'])
                if val is False:
                    skipped += 1
                    continue

            if 'error_level' in kwargs:
                skipped += 1
                continue

            if 'normalize_functions' in kwargs:
                skipped += 1
                continue

            if 'pad' in kwargs or 'indent' in kwargs:
                skipped += 1
                continue

            # Get write= and read= dialect (string values, not dicts)
            write_dialect = None
            read_dialect = None

            if 'write' in kwargs:
                write_dialect = get_string_value(kwargs['write'])
                # write=None means generic
                if write_dialect is None and isinstance(kwargs['write'], ast.Constant) and kwargs['write'].value is None:
                    write_dialect = ""

            if 'read' in kwargs:
                read_dialect = get_string_value(kwargs['read'])

            line = getattr(node, 'lineno', 0)

            if write_dialect is not None and write_dialect != "":
                # Transpilation test: parse generic, write to dialect
                transpilation_tests.append({
                    'sql': sql,
                    'expected': target,
                    'write': write_dialect,
                    'line': line,
                })
            elif read_dialect is not None:
                # Read test: parse with read dialect, generate generic
                transpilation_tests.append({
                    'sql': sql,
                    'expected': target,
                    'read': read_dialect,
                    'line': line,
                })
            else:
                # Normalization test: no dialect, just parse/generate generic
                normalization_tests.append({
                    'sql': sql,
                    'expected': target,
                    'line': line,
                })

        elif method_name == 'assertEqual':
            # Look for assertEqual(transpile(sql, **kwargs)[0], target) pattern
            if len(node.args) < 2:
                continue

            # First arg should be transpile(sql)[0] - a subscript
            first_arg = node.args[0]
            if not isinstance(first_arg, ast.Subscript):
                continue

            # The subscript value should be a call to transpile
            if not isinstance(first_arg.value, ast.Call):
                continue

            transpile_call = first_arg.value
            if not isinstance(transpile_call.func, ast.Name) or transpile_call.func.id != 'transpile':
                continue

            if len(transpile_call.args) < 1:
                continue

            sql = get_string_value(transpile_call.args[0])
            target = get_string_value(node.args[1])
            if not sql or not target:
                continue

            # Parse kwargs on the transpile() call
            kwargs = {}
            for kw in transpile_call.keywords:
                if kw.arg:
                    kwargs[kw.arg] = kw.value

            # Skip tests with special params
            if 'pretty' in kwargs or 'error_level' in kwargs or 'normalize_functions' in kwargs:
                skipped += 1
                continue

            if 'unsupported_level' in kwargs:
                skipped += 1
                continue

            # Get read/write dialects
            write_dialect = None
            read_dialect = None

            if 'write' in kwargs:
                write_dialect = get_string_value(kwargs['write'])
            if 'read' in kwargs:
                read_dialect = get_string_value(kwargs['read'])

            line = getattr(node, 'lineno', 0)

            if write_dialect:
                transpilation_tests.append({
                    'sql': sql,
                    'expected': target,
                    'write': write_dialect,
                    'line': line,
                })
            elif read_dialect:
                transpilation_tests.append({
                    'sql': sql,
                    'expected': target,
                    'read': read_dialect,
                    'line': line,
                })
            else:
                # No dialect = normalization test
                normalization_tests.append({
                    'sql': sql,
                    'expected': target,
                    'line': line,
                })

    return {
        'normalization': normalization_tests,
        'transpilation': transpilation_tests,
        'skipped': skipped,
    }


def extract_parser_tests(filepath: str) -> Dict[str, Any]:
    """
    Extract test cases from test_parser.py.

    Extracts:
    - assertEqual(parse_one(sql).sql(), expected) round-trip tests
    - assertEqual(parse_one(sql, read=dialect).sql(dialect=dialect), expected) dialect round-trips
    - assertRaises(ParseError) -> parse_one(sql) error tests

    Skips tests with error_level, into=, etc.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"  Syntax error in {filepath}: {e}", file=sys.stderr)
        return {'roundtrips': [], 'errors': []}

    roundtrip_tests = []
    error_tests = []

    # We need to visit the tree more carefully for assertRaises blocks
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method_name = node.func.attr

            if method_name == 'assertEqual':
                # Look for assertEqual(parse_one(sql).sql(), expected)
                if len(node.args) < 2:
                    continue

                first_arg = node.args[0]

                # First arg should be parse_one(sql).sql() - a call to .sql()
                if not isinstance(first_arg, ast.Call):
                    continue
                if not isinstance(first_arg.func, ast.Attribute):
                    continue
                if first_arg.func.attr != 'sql':
                    continue

                # The value of .sql() should be parse_one(sql)
                parse_call = first_arg.func.value
                if not isinstance(parse_call, ast.Call):
                    continue

                # Check it's parse_one
                if isinstance(parse_call.func, ast.Name) and parse_call.func.id == 'parse_one':
                    pass
                else:
                    continue

                if len(parse_call.args) < 1:
                    continue

                sql = get_string_value(parse_call.args[0])
                expected = get_string_value(node.args[1])
                if not sql or not expected:
                    continue

                # Parse kwargs on parse_one()
                parse_kwargs = {}
                for kw in parse_call.keywords:
                    if kw.arg:
                        parse_kwargs[kw.arg] = kw.value

                # Skip tests with error_level, into=
                if 'error_level' in parse_kwargs or 'into' in parse_kwargs:
                    continue

                # Parse kwargs on .sql() call
                sql_kwargs = {}
                for kw in first_arg.keywords:
                    if kw.arg:
                        sql_kwargs[kw.arg] = kw.value

                # Skip tests with pretty=True on .sql()
                if 'pretty' in sql_kwargs:
                    val = get_bool_value(sql_kwargs['pretty'])
                    if val is True:
                        continue

                read_dialect = None
                if 'read' in parse_kwargs:
                    read_dialect = get_string_value(parse_kwargs['read'])

                # If .sql(dialect=X), capture write dialect
                write_dialect = None
                if 'dialect' in sql_kwargs:
                    write_dialect = get_string_value(sql_kwargs['dialect'])

                line = getattr(node, 'lineno', 0)

                roundtrip_tests.append({
                    'sql': sql,
                    'expected': expected,
                    'read': read_dialect,
                    'write': write_dialect,
                    'line': line,
                })

        # Look for assertRaises(ParseError) blocks
        if isinstance(node, ast.With):
            for item in node.items:
                ctx = item.context_expr
                if not isinstance(ctx, ast.Call):
                    continue
                if not isinstance(ctx.func, ast.Attribute):
                    continue
                if ctx.func.attr != 'assertRaises':
                    continue
                if len(ctx.args) < 1:
                    continue

                # Check it's ParseError
                err_arg = ctx.args[0]
                if isinstance(err_arg, ast.Name) and err_arg.id == 'ParseError':
                    pass
                else:
                    continue

                # Skip if there's an 'as ctx' binding (these check error details, not simple error tests)
                if item.optional_vars is not None:
                    continue

                # Look inside the with block for parse_one(sql) calls
                for stmt in node.body:
                    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                        call = stmt.value
                        if isinstance(call.func, ast.Name) and call.func.id == 'parse_one':
                            if len(call.args) >= 1:
                                sql = get_string_value(call.args[0])
                                if sql:
                                    # Check for read= dialect
                                    read_dialect = None
                                    for kw in call.keywords:
                                        if kw.arg == 'read':
                                            read_dialect = get_string_value(kw.value)
                                    # Skip if error_level is used
                                    has_error_level = any(kw.arg == 'error_level' for kw in call.keywords)
                                    if has_error_level:
                                        continue
                                    line = getattr(call, 'lineno', 0)
                                    error_tests.append({
                                        'sql': sql,
                                        'read': read_dialect,
                                        'line': line,
                                    })

    return {
        'roundtrips': roundtrip_tests,
        'errors': error_tests,
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

        # Skip non-test files
        if dialect_name in ('__init__',):
            continue

        # test_dialect.py is the generic dialect test file — extract it as "generic"
        if dialect_name == 'dialect':
            dialect_name = 'generic'

        print(f"  {dialect_name}...", end=' ')

        tests = extract_tests_from_file(str(test_file))

        # For generic (test_dialect.py), the dialect attribute is None, use empty string
        if dialect_name == 'generic':
            tests['dialect'] = ''
        elif not tests['dialect']:
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

    # =========================================================================
    # Extract transpile tests from test_transpile.py
    # =========================================================================
    print()
    print("Transpile tests (test_transpile.py):")
    print("-" * 60)

    transpile_file = sqlglot_tests_dir / 'test_transpile.py'
    if transpile_file.exists():
        transpile_data = extract_transpile_tests(str(transpile_file))

        with open(extracted_dir / 'transpile.json', 'w') as f:
            json.dump(transpile_data, f, indent=2, ensure_ascii=False)

        norm_count = len(transpile_data['normalization'])
        trans_count = len(transpile_data['transpilation'])
        skip_count = transpile_data['skipped']
        print(f"  Normalization: {norm_count}")
        print(f"  Transpilation: {trans_count}")
        print(f"  Skipped: {skip_count}")
    else:
        print("  test_transpile.py not found!")

    # =========================================================================
    # Extract parser tests from test_parser.py
    # =========================================================================
    print()
    print("Parser tests (test_parser.py):")
    print("-" * 60)

    parser_file = sqlglot_tests_dir / 'test_parser.py'
    if parser_file.exists():
        parser_data = extract_parser_tests(str(parser_file))

        with open(extracted_dir / 'parser.json', 'w') as f:
            json.dump(parser_data, f, indent=2, ensure_ascii=False)

        rt_count = len(parser_data['roundtrips'])
        err_count = len(parser_data['errors'])
        print(f"  Roundtrips: {rt_count}")
        print(f"  Errors: {err_count}")
    else:
        print("  test_parser.py not found!")

    print()
    print(f"Output written to: {extracted_dir}")


if __name__ == '__main__':
    main()
