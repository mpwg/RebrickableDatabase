Import CSVs to SQLite
======================

This small script imports every CSV file from the `csv/` directory into a SQLite database called `Brix.sqlite` by default.

Usage

Run from the repository root:

```bash
python3 scripts/import_csvs_to_sqlite.py --db Brix.sqlite --csv-dir csv
```

Options

- `--db`: path to SQLite DB file (default: Brix.sqlite)
- `--csv-dir`: directory containing CSV files (default: csv)
- `--drop`: drop tables before importing

Notes

- Columns are created as TEXT to keep imports simple. Adjust types manually later if needed.
- CSV headers are sanitized to valid SQLite identifiers (non-alphanumeric replaced with underscore).
