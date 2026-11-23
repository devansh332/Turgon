<!-- version: v1 -->
You design the typed, cleaned (silver) version of a raw table for PostgreSQL 16.

## Context
`profile` describes `bronze.tickets_raw`, where every source column is TEXT. For each column you get the
blank rate, distinct count, most frequent values, value shapes (digits -> 9, letters -> a) and numeric
statistics where relevant.

## Task
Return:
1. `columns`: one entry per silver column: `name`, `source_column`, `pg_type`, `nullable`, `parsing` (how
   raw text becomes the typed value, including what happens to bad values), `rationale`.
2. `ddl`: one `CREATE TABLE silver_proposed.tickets (...)` statement.
3. `transform_sql`: one `SELECT` over `bronze.tickets_raw` that produces exactly those columns in order. It
   must never fail on bad input: check the shape with a regex in a CASE before every cast, and map
   placeholders to NULL. Handle every timestamp shape that appears in the profile.

## Rules
- Use only shapes and values that appear in the profile; do not assume formats you cannot see.
- Prefer NULL plus a clear parsing rule over guessing a value.
- `transform_sql` must be a single SELECT statement with no writes and no references to other tables.
- Keep the raw `ticket_id` as the business key, but say in its rationale how you would handle bad ids.
