from medallion.config import SQL_DIR, Config
from medallion.db import connect
from medallion.pipeline.runs import stage_run
from medallion.seeds import Seeds, rules_version

MARTS = ["sla_compliance", "vendor_performance", "backlog_daily"]
# gold column -> bronze source column, for applying PII tags (same names in ticket_facts and the marts)
FACT_SOURCES = {"assignee": "assigned_to", "assignee_type": "assigned_to", "building": "building", "cost": "cost"}


def grant_bi_access(conn) -> list[str]:
    """medallion_bi reads every gold column whose source column is not tagged PII (rule tags from landing in
    meta.column_tags, overridden by approved tagging-agent seeds). Returns the hidden `object.column`s."""
    conn.execute(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='medallion_bi') "
        "THEN CREATE ROLE medallion_bi NOLOGIN; END IF; END $$"
    )
    conn.execute("GRANT USAGE ON SCHEMA gold TO medallion_bi")
    tags = {
        r["column_name"]: r["pii"]
        for r in conn.execute("SELECT column_name, pii FROM meta.column_tags WHERE table_name='bronze.tickets_raw'")
    }
    for key, tag in Seeds().column_tags.items():  # approvals made since the last ingest
        tags[key.removeprefix("bronze.tickets_raw.")] = bool(tag.get("pii"))
    pii = {c for c, is_pii in tags.items() if is_pii}
    hidden = []
    for obj in ["ticket_facts", *MARTS]:
        # pg_attribute, not information_schema: the latter omits materialized views
        columns = [
            r["attname"]
            for r in conn.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnum > 0"
                " AND NOT attisdropped ORDER BY attnum",
                (f"gold.{obj}",),
            ).fetchall()
        ]
        visible = [c for c in columns if FACT_SOURCES.get(c) not in pii]
        hidden += [f"{obj}.{c}" for c in columns if c not in visible]
        conn.execute(f"GRANT SELECT ({', '.join(visible)}) ON gold.{obj} TO medallion_bi")
    return hidden


def build(cfg: Config) -> None:
    with stage_run(cfg, "gold", rules_version()) as run, connect(cfg) as conn:
        for path in sorted((SQL_DIR / "gold").glob("*.sql")):
            conn.execute(path.read_text())
        counts = {m: conn.execute(f"SELECT count(*) AS n FROM gold.{m}").fetchone()["n"] for m in MARTS}
        run.rows_in = conn.execute("SELECT count(*) AS n FROM gold.ticket_facts").fetchone()["n"]
        run.details["bi_hidden_pii_columns"] = grant_bi_access(conn)
        conn.commit()
        run.rows_out = sum(counts.values())
        run.details["mart_rows"] = counts
