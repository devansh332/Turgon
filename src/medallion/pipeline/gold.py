from medallion.config import SQL_DIR, Config
from medallion.db import connect
from medallion.pipeline.runs import stage_run
from medallion.seeds import rules_version

MARTS = ["sla_compliance", "vendor_performance", "backlog_daily"]


def build(cfg: Config) -> None:
    with stage_run(cfg, "gold", rules_version()) as run, connect(cfg) as conn:
        for path in sorted((SQL_DIR / "gold").glob("*.sql")):
            conn.execute(path.read_text())
        counts = {m: conn.execute(f"SELECT count(*) AS n FROM gold.{m}").fetchone()["n"] for m in MARTS}
        run.rows_in = conn.execute("SELECT count(*) AS n FROM gold.ticket_facts").fetchone()["n"]
        conn.commit()
        run.rows_out = sum(counts.values())
        run.details["mart_rows"] = counts
