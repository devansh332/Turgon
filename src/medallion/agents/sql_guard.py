"""Guardrail for agent-generated SQL: parse with sqlglot and allow only a single read-only SELECT over
whitelisted tables, using an allowlist of functions. Execution additionally happens in a READ ONLY transaction
with a timeout, as a role that can only read bronze.tickets_raw (db.read_only), so a parser gap is not enough
to reach other data."""

import sqlglot
from sqlglot import exp

from medallion.config import SOURCE_COLUMNS

ALLOWED_TABLES = {"bronze.tickets_raw"}
ALLOWED_COLUMNS = set(SOURCE_COLUMNS) | {"_bronze_id", "_extra", "_row_hash", "_ingested_at"}
# sqlglot maps standard SQL functions (lower, coalesce, cast, regexp_like, split_part...) to typed nodes.
# Anything it does not recognise arrives as exp.Anonymous; those must be on this list.
ALLOWED_ANONYMOUS = {
    "btrim",
    "char_length",
    "jsonb_typeof",
    "regexp_match",
    "regexp_matches",
    "to_number",
    "to_timestamp",
    "to_date",
    "array_length",
    "octet_length",
}
DENIED_TYPED = {exp.CurrentUser}
_WRITE_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.Command,
    exp.Into,
    exp.Merge,
    exp.TruncateTable,
)


class UnsafeSQL(ValueError):
    pass


def _visible_ctes(node: exp.Expression) -> set[str]:
    names: set[str] = set()
    parent = node.parent
    while parent is not None:
        with_ = (parent.args.get("with_") or parent.args.get("with")) if isinstance(parent, exp.Query) else None
        if with_ is not None:
            names |= {cte.alias for cte in with_.expressions}
        parent = parent.parent
    return names


def validate_select(sql: str, allowed_tables: set[str] = ALLOWED_TABLES) -> str:
    try:
        statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise UnsafeSQL(f"does not parse: {exc}") from exc
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise UnsafeSQL("must be exactly one SELECT statement")
    tree = statements[0]
    if any(isinstance(node, _WRITE_NODES) for node in tree.walk()):
        raise UnsafeSQL("write or DDL operations are not allowed")
    for table in tree.find_all(exp.Table):
        qualified = f"{table.db}.{table.name}" if table.db else table.name
        if qualified in allowed_tables:
            continue
        if not table.db and table.name in _visible_ctes(table):
            continue
        raise UnsafeSQL(f"table not allowed: {qualified}")
    for fn in tree.find_all(exp.Func):
        if isinstance(fn, exp.Anonymous):
            if fn.name.lower() not in ALLOWED_ANONYMOUS:
                raise UnsafeSQL(f"function not allowed: {fn.name.lower()}")
        elif type(fn) in DENIED_TYPED:
            raise UnsafeSQL(f"function not allowed: {fn.sql_name().lower()}")
    return sql


def predicate_query(predicate: str) -> str:
    """Wrap a boolean predicate over bronze columns into a validated SELECT of matching row ids."""
    sql = validate_select(f"SELECT _bronze_id FROM bronze.tickets_raw WHERE {predicate}")
    tree = sqlglot.parse_one(sql, read="postgres")
    unknown = {c.name for c in tree.find_all(exp.Column)} - ALLOWED_COLUMNS
    if unknown:
        raise UnsafeSQL(f"unknown columns: {sorted(unknown)}")
    return sql
