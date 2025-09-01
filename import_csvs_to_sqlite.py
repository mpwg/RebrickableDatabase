#!/usr/bin/env python3
"""Import all CSV files from a directory into a SQLite database.

Creates (or replaces) tables named after each CSV file (filename without .csv).
All columns are created as TEXT to keep the import simple and robust.

Usage:
    python3 scripts/import_csvs_to_sqlite.py --db Brix.sqlite --csv-dir csv
"""
import argparse
import csv
import os
import sqlite3
import sys
import re
from glob import glob
from typing import List, Tuple, Optional, Dict, Any


def sanitize_name(name: str) -> str:
    # Keep letters, numbers and underscores. Replace others with underscore.
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    # If it starts with a digit, prefix with an underscore
    if re.match(r"^[0-9]", name):
        name = "_" + name
    return name


def infer_column_types(csv_path: str, cols: List[str], sample_size: int = 1000) -> List[str]:
    """Infer simple SQLite types for columns by sampling rows.

    Returns a list with values: 'INTEGER', 'REAL', or 'TEXT'.
    """
    types = ['INTEGER'] * len(cols)
    seen = 0
    with open(csv_path, newline='', encoding='utf-8') as fh:
        # Use a conservative, reliable default delimiter. Sniffer can mis-detect and cause errors.
        delimiter = ','
        reader = csv.reader(fh, delimiter=delimiter)
        try:
            next(reader)
        except StopIteration:
            return ['TEXT'] * len(cols)

        for row in reader:
            if seen >= sample_size:
                break
            seen += 1
            for i, v in enumerate(row[: len(cols)]):
                if v == '' or v is None:
                    continue
                # Already text, no need to check
                if types[i] == 'TEXT':
                    continue
                # Check integer
                try:
                    int(v)
                    continue
                except Exception:
                    pass
                # Check float
                try:
                    float(v)
                    types[i] = 'REAL' if types[i] != 'TEXT' else 'TEXT'
                    continue
                except Exception:
                    types[i] = 'TEXT'

    # fallback: if nothing seen, make TEXT
    types = [t if seen > 0 else 'TEXT' for t in types]
    return types


def detect_primary_key(csv_path: str, cols: List[str], size_limit_rows: int = 50000000) -> Tuple[str, bool]:
    """Detect a primary key column by checking uniqueness across the file.

    Returns (column_name_or_empty, True_if_detected)
    This can be memory-heavy; we guard with a row-count limit.
    """
    # First do a cheap pass to count rows (without loading all data into memory)
    row_count = 0
    with open(csv_path, newline='', encoding='utf-8') as fh:
        delimiter = ','
        reader = csv.reader(fh, delimiter=delimiter)
        try:
            next(reader)
        except StopIteration:
            return ('', False)
        for _ in reader:
            row_count += 1
            if row_count > size_limit_rows:
                print(f"  Skipping PK detection: file has >{size_limit_rows} rows (use --detect-pk to force)")
                return ('', False)

    # If small enough, check each column for uniqueness
    unique_candidates = {i: set() for i in range(len(cols))}
    with open(csv_path, newline='', encoding='utf-8') as fh:
        delimiter = ','
        reader = csv.reader(fh, delimiter=delimiter)
        header = next(reader)
        for row in reader:
            for i in range(len(cols)):
                v = row[i] if i < len(row) else None
                if v is None or v == '':
                    # presence of nulls disqualifies primary key
                    unique_candidates.pop(i, None)
                    continue
                s = unique_candidates.get(i)
                if s is None:
                    continue
                if v in s:
                    unique_candidates.pop(i, None)
                else:
                    s.add(v)
            if not unique_candidates:
                break

    if unique_candidates:
        # prefer columns named id or ending with _id
        for i, name in enumerate(cols):
            if i in unique_candidates and (name.lower() == 'id' or name.lower().endswith('_id')):
                return (name, True)
        # otherwise pick the first remaining
        idx = next(iter(unique_candidates))
        return (cols[idx], True)

    return ('', False)


def import_csv_to_table(conn: sqlite3.Connection, csv_path: str, table_name: str,
                        *, detect_pk: bool = False, max_rows: Optional[int] = None, skip_large: Optional[int] = None,
                        pre_meta: Optional[Dict[str, Any]] = None, all_meta: Optional[Dict[str, Any]] = None):
    print(f"Importing '{csv_path}' -> table '{table_name}'")

    # Optionally skip very large files by simple line count
    if skip_large is not None:
        with open(csv_path, newline='', encoding='utf-8') as fh:
            total = sum(1 for _ in fh) - 1
        if total > skip_large:
            print(f"  Skipping large file ({total} rows) > {skip_large}")
            return

    # If precomputed metadata is provided, use it (this allows resolving foreign keys across files)
    if pre_meta is not None:
        cols = pre_meta['cols']
        cols_sanitized = pre_meta['cols_sanitized']
        inferred = pre_meta['inferred']
        pk_col = pre_meta.get('pk_col', '')
        pk_detected = pre_meta.get('pk_detected', False)
    else:
        # Read header and sanitize columns
        with open(csv_path, newline='', encoding='utf-8') as fh:
            delimiter = ','
            reader = csv.reader(fh, delimiter=delimiter)
            try:
                header = next(reader)
            except StopIteration:
                print(f"  Skipping empty file: {csv_path}")
                return

            cols = [c.strip() for c in header]
            if any(c == '' for c in cols):
                cols = [c if c != '' else f"col_{i+1}" for i, c in enumerate(cols)]

            cols_sanitized = [sanitize_name(c) for c in cols]

        # Infer column types
        inferred = infer_column_types(csv_path, cols)

        # Detect primary key if requested
        pk_col = ''
        pk_detected = False
        if detect_pk:
            pk_col, pk_detected = detect_primary_key(csv_path, cols)
            if pk_detected:
                pk_col = sanitize_name(pk_col)

    # Build CREATE TABLE statement with inferred types and optional PK and foreign key constraints
    col_defs_parts = []
    fk_parts = []
    for name, typ in zip(cols_sanitized, inferred):
        part = f'"{name}" {typ}'
        if pk_detected and name == pk_col:
            part += ' PRIMARY KEY'
        col_defs_parts.append(part)

        # Add a foreign key clause when column looks like a foreign key (ends with _id) and we have metadata
        if name.lower().endswith('_id') and all_meta is not None:
            base = name[:-3]
            # Try candidate table names: base, base+'s', base+'es'
            candidates = [base, base + 's', base + 'es']
            ref_table = None
            for c in candidates:
                if c in all_meta:
                    ref_table = c
                    break
            if ref_table is None:
                # Try matching plural forms of existing tables to the base (e.g., base='set' -> 'sets')
                for t in all_meta.keys():
                    if t == base or t.endswith(base + 's') or t.endswith(base):
                        ref_table = t
                        break
            if ref_table is not None:
                # Choose referenced column: prefer detected PK, otherwise 'id' if present, else first column
                ref_pk = all_meta[ref_table].get('pk_col', '')
                if not ref_pk:
                    if 'id' in all_meta[ref_table]['cols_sanitized']:
                        ref_pk = 'id'
                    else:
                        ref_pk = all_meta[ref_table]['cols_sanitized'][0]
                fk_parts.append(f'FOREIGN KEY ("{name}") REFERENCES "{ref_table}"("{ref_pk}")')

    cur = conn.cursor()
    create_body = ", ".join(col_defs_parts + fk_parts)
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({create_body});'
    cur.execute(create_sql)

    # Use idempotent inserts to avoid failing on duplicates when re-running imports.
    insert_sql = f'INSERT OR IGNORE INTO "{table_name}" ({", ".join([f"\"{c}\"" for c in cols_sanitized])}) VALUES ({", ".join(["?" for _ in cols_sanitized])})'

    # Stream rows and insert with batching
    batch = []
    batch_size = 1000
    rows = 0
    with open(csv_path, newline='', encoding='utf-8') as fh:
        # Use conservative default delimiter for streaming rows
        delimiter = ','
        reader = csv.reader(fh, delimiter=delimiter)
        try:
            next(reader)
        except StopIteration:
            return

        for row in reader:
            if max_rows is not None and rows >= max_rows:
                break

            if len(row) < len(cols_sanitized):
                row = row + [None] * (len(cols_sanitized) - len(row))
            elif len(row) > len(cols_sanitized):
                row = row[: len(cols_sanitized)]

            processed = []
            for i, v in enumerate(row[: len(cols_sanitized)]):
                if v is None or v == '':
                    processed.append(None)
                    continue
                t = inferred[i]
                if t == 'INTEGER':
                    try:
                        processed.append(int(v))
                    except Exception:
                        processed.append(v)
                elif t == 'REAL':
                    try:
                        processed.append(float(v))
                    except Exception:
                        processed.append(v)
                else:
                    processed.append(v)

            batch.append(processed)
            rows += 1

            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                conn.commit()
                batch = []

            if max_rows is not None and rows >= max_rows:
                # commit any remaining rows in batch and stop
                if batch:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    batch = []
                break

        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()

    print(f"  Inserted {rows} rows into '{table_name}'")

    # Create indexes for columns that look like foreign keys (end with _id)
    for col in cols_sanitized:
        if col.lower().endswith('_id') and (not pk_detected or col != pk_col):
            idx_name = f'idx_{table_name}_{col}'
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ("{col}");')
                conn.commit()
                print(f"  Created index {idx_name} on {col}")
            except Exception as e:
                print(f"  Failed to create index on {col}: {e}")


def main(argv=None):
    parser = argparse.ArgumentParser(description='Import CSV files into SQLite database')
    parser.add_argument('--db', default='Brix.sqlite', help='SQLite database file to create/use')
    parser.add_argument('--csv-dir', default='csv', help='Directory containing CSV files')
    parser.add_argument('--drop', action='store_true', help='Drop existing tables before import')
    parser.add_argument('--detect-pk', action='store_true', help='Try to detect a primary key column')
    parser.add_argument('--max-rows', type=int, default=None, help='Maximum rows to import per file')
    parser.add_argument('--skip-large', type=int, default=None, help='Skip files with more than this many rows')
    args = parser.parse_args(argv)

    csv_dir = os.path.abspath(args.csv_dir)
    if not os.path.isdir(csv_dir):
        print(f"CSV directory not found: {csv_dir}")
        sys.exit(2)

    db_path = os.path.abspath(args.db)
    print(f"Creating/opening SQLite DB at: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = OFF;')
    conn.execute('PRAGMA synchronous = NORMAL;')
    conn.execute('PRAGMA journal_mode = WAL;')

    try:
        csv_files = sorted(glob(os.path.join(csv_dir, '*.csv')))
        if not csv_files:
            print(f"No CSV files found in {csv_dir}")
            return

        # First pass: gather metadata for all CSVs so we can resolve foreign keys across tables
        all_meta: Dict[str, Any] = {}
        file_map: Dict[str, str] = {}
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            table = os.path.splitext(filename)[0]
            table = sanitize_name(table)
            file_map[table] = csv_path

            # Read header
            with open(csv_path, newline='', encoding='utf-8') as fh:
                delimiter = ','
                reader = csv.reader(fh, delimiter=delimiter)
                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                cols = [c.strip() for c in header]
                if any(c == '' for c in cols):
                    cols = [c if c != '' else f"col_{i+1}" for i, c in enumerate(cols)]
                cols_sanitized = [sanitize_name(c) for c in cols]

            inferred = infer_column_types(csv_path, cols)
            pk_col = ''
            pk_detected = False
            if args.detect_pk:
                pk_col, pk_detected = detect_primary_key(csv_path, cols)
                if pk_detected:
                    pk_col = sanitize_name(pk_col)

            all_meta[table] = {
                'cols': cols,
                'cols_sanitized': cols_sanitized,
                'inferred': inferred,
                'pk_col': pk_col,
                'pk_detected': pk_detected,
            }

        # Optionally drop tables first
        if args.drop:
            for t in all_meta.keys():
                conn.execute(f'DROP TABLE IF EXISTS "{t}";')

        # Import files using metadata to generate FK constraints
        for table, meta in all_meta.items():
            csv_path = file_map[table]
            import_csv_to_table(conn, csv_path, table, detect_pk=args.detect_pk,
                                max_rows=args.max_rows, skip_large=args.skip_large,
                                pre_meta=meta, all_meta=all_meta)

        # After import, enable foreign key enforcement and check for violations
        conn.execute('PRAGMA foreign_keys = ON;')
        cur = conn.execute('PRAGMA foreign_key_check;')
        violations = cur.fetchall()
        if violations:
            print('\nForeign key check found violations:')
            for v in violations:
                # v is (table, rowid, parent, fkid)
                print(f'  Table {v[0]} rowid={v[1]} references missing parent in {v[2]} (fk={v[3]})')
        else:
            print('\nForeign key check passed: no violations')

    finally:
        conn.close()


if __name__ == '__main__':
    main()
