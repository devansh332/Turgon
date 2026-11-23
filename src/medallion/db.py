from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from medallion.config import SQL_DIR, Config


def connect(cfg: Config, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(cfg.database_url, autocommit=autocommit, row_factory=dict_row)


def apply_ddl(conn: psycopg.Connection) -> None:
    for path in sorted(SQL_DIR.glob("*.sql")):
        conn.execute(path.read_text())
    conn.commit()


@contextmanager
def read_only(conn: psycopg.Connection, timeout_ms: int = 5000):
    """Run untrusted (agent-generated) SQL: read-only transaction with a hard timeout, always rolled back,
    as the unprivileged medallion_agent role (SELECT on bronze.tickets_raw only)."""
    with conn.transaction(force_rollback=True):
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
        conn.execute("SET LOCAL ROLE medallion_agent")
        yield conn
