Import CSVs into SQLite
=======================

This repository includes a small script, `import_csvs_to_sqlite.py`, which imports all CSV files from a directory (default `csv/`) into a SQLite database (default `Brix.sqlite`). Each CSV becomes a table named after the file (filename without `.csv`, sanitized to a valid SQLite identifier).

Source of CSV files
-------------------

The CSV files included in the `csv/` folder were downloaded from [Rebrickable's downloads page](https://rebrickable.com/downloads/). Rebrickable is a community-maintained database of LEGO sets, parts, minifigures, and inventories — a helpful resource for builders, collectors, and developers. Thanks to Rebrickable for making the data available.

Quick usage
-----------

Run from the repository root:

```bash
python3 import_csvs_to_sqlite.py --db Brix.sqlite --csv-dir csv
```

What the script does
---------------------

- Creates (if missing) one table per CSV file. Table name = sanitized filename without `.csv`.
- Infers simple column types by sampling rows: INTEGER, REAL, or TEXT.
- Sanitizes column names (non-alphanumeric characters replaced with underscores; names starting with a digit are prefixed with `_`).
- Optionally attempts to detect a primary key column (unique, non-null values) when `--detect-pk` is passed.
- Inserts rows using `INSERT OR IGNORE` so re-running the import is idempotent for duplicate rows.
- Streams input and inserts in batches to limit memory usage.
- Automatically creates indexes for columns that look like foreign keys (column names ending with `_id`).
- Sets a few SQLite pragmas for reasonable import performance (foreign_keys OFF, synchronous=NORMAL, journal_mode=WAL).

Command-line options
---------------------

- `--db` (default: `Brix.sqlite`) — path to the SQLite database file to create/use.
- `--csv-dir` (default: `csv`) — directory containing `.csv` files to import.
- `--drop` — drop existing tables before importing.
- `--detect-pk` — attempt to detect a primary key column for each CSV (may be skipped for very large files).
- `--max-rows` — maximum rows to import per file (useful for testing or sampling large files).
- `--skip-large` — skip files with more than this many rows (simple pre-check by counting lines).

Notes & caveats
--------------

- Column type inference is conservative and sample-based. If a column contains mixed values, it will fall back to TEXT.
- Primary-key detection scans files and can be memory/time-consuming; the script will skip detection for files with very large row counts unless requested.
- The script treats empty strings as NULL when inserting.
- If CSV headers contain empty names, they are auto-renamed to `col_1`, `col_2`, etc., before sanitization.

Example runs
------------

```bash
# Import everything into the default database
python3 import_csvs_to_sqlite.py

# Drop tables and recreate them from CSVs
python3 import_csvs_to_sqlite.py --drop

# Try to detect primary keys and skip files with >1_000_000 rows
python3 import_csvs_to_sqlite.py --detect-pk --skip-large 1000000
```

License
-------

See the project `LICENSE` file.

