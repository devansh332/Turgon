<!-- version: v1 -->
A new column appeared in a source file for `bronze.tickets_raw`. Bronze stored it losslessly in the JSONB
column `_extra`. You draft the silver migration so a human can review it.

## Context
You receive the column name, sample values and the current silver column names.

## Task
Return:
- `silver_column`: snake_case name for silver (must not collide with an existing column).
- `pg_type`: the PostgreSQL type the samples support.
- `alter_sql`: one `ALTER TABLE silver.tickets ADD COLUMN IF NOT EXISTS ...` statement.
- `extract_sql`: a PostgreSQL expression over the bronze row that yields the typed value, reading
  `_extra->>'<column>'`, guarded with a regex before any cast so bad values become NULL.
- `rationale` and `confidence` (0-1).

## Rules
- Never drop, rename or retype existing columns.
- If the samples are too few or too inconsistent to pick a type, use text and say so in the rationale.
