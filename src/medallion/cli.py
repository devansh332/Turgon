import argparse
import json
import sys
from pathlib import Path

from medallion.config import DEFAULT_SOURCE, Config
from medallion.db import apply_ddl, connect


def cmd_init(cfg: Config, args) -> None:
    with connect(cfg) as conn:
        apply_ddl(conn)


def cmd_ingest(cfg: Config, args) -> None:
    from medallion.pipeline import bronze

    cmd_init(cfg, args)
    bronze.ingest(cfg, Path(args.file))


def cmd_silver(cfg: Config, args) -> None:
    from medallion.pipeline import silver

    silver.build(cfg, full=args.full)


def cmd_gold(cfg: Config, args) -> None:
    from medallion.pipeline import gold

    gold.build(cfg)


def cmd_reconcile(cfg: Config, args) -> None:
    from medallion.pipeline import reconcile

    reconcile.run_checks(cfg)


def cmd_run(cfg: Config, args) -> None:
    cmd_ingest(cfg, args)
    cmd_silver(cfg, args)
    cmd_gold(cfg, args)
    cmd_reconcile(cfg, args)
    cmd_metrics(cfg, args)


def cmd_agents(cfg: Config, args) -> None:
    from medallion.agents import classifier, dq_agent, schema_agent, tagging
    from medallion.agents.llm import LLMClient

    llm = LLMClient(cfg)
    runners = {"classifier": classifier.run, "dq": dq_agent.run, "schema": schema_agent.run, "tagging": tagging.run}
    # autocommit: an agent waits minutes on the model, and an open transaction would block DDL and silver
    with connect(cfg, autocommit=True) as conn:
        for name in args.only or runners:
            summary = runners[name](conn, llm, cfg.auto_approve_confidence)
            print(json.dumps({"agent": name, **summary}, default=str))
        from medallion import proposals

        proposals.export_seeds(conn)


def cmd_proposals(cfg: Config, args) -> None:
    from medallion import proposals

    with connect(cfg) as conn:
        if args.action == "list":
            for p in proposals.list_proposals(conn, None if args.status == "all" else args.status):
                print(
                    f"#{p['proposal_id']:<5} {p['status']:<13} {p['risk']:<4} {p['kind']:<20} {p['item_key'][:50]:<50} "
                    f"conf={p['confidence']} {json.dumps(p['payload'], default=str)[:140]}"
                )
                if args.verbose:
                    print(f"        why: {p['rationale']}")
        elif args.action in ("approve", "reject"):
            ids = [int(i) for i in args.ids]
            if args.all_kind:
                ids += [p["proposal_id"] for p in proposals.list_proposals(conn) if p["kind"] == args.all_kind]
            n = proposals.decide(conn, ids, "approved" if args.action == "approve" else "rejected", args.by)
            print(f"{'approved' if args.action == 'approve' else 'rejected'} {n} proposal(s)")
        proposals.export_seeds(conn)


def cmd_metrics(cfg: Config, args) -> None:
    from medallion.pipeline import report

    with connect(cfg) as conn:
        print(report.metrics(conn))


def cmd_lineage(cfg: Config, args) -> None:
    from medallion.pipeline import report

    with connect(cfg) as conn:
        print(report.lineage(conn, args.key))


def cmd_eval(cfg: Config, args) -> None:
    from medallion.agents import evals

    sys.exit(0 if evals.run(cfg, promote=args.promote) else 1)


def main() -> None:
    parser = argparse.ArgumentParser(prog="medallion", description="AI-assisted medallion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="ingest -> silver -> gold -> reconcile -> metrics")
    p.add_argument("--file", default=str(DEFAULT_SOURCE))
    p.add_argument("--full", action="store_true", help="rebuild silver from all of bronze")
    p.set_defaults(func=cmd_run)

    sub.add_parser("init", help="create schemas and tables").set_defaults(func=cmd_init)

    p = sub.add_parser("ingest", help="load a CSV into bronze")
    p.add_argument("--file", default=str(DEFAULT_SOURCE))
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("silver", help="bronze -> silver")
    p.add_argument("--full", action="store_true")
    p.set_defaults(func=cmd_silver)

    sub.add_parser("gold", help="silver -> gold marts").set_defaults(func=cmd_gold)
    sub.add_parser("reconcile", help="cross-layer reconciliation checks").set_defaults(func=cmd_reconcile)

    p = sub.add_parser("agents", help="run agents; they write proposals, never silver")
    p.add_argument("--only", nargs="*", choices=["classifier", "dq", "schema", "tagging"])
    p.set_defaults(func=cmd_agents)

    p = sub.add_parser("proposals", help="review agent proposals (human-in-the-loop gate)")
    p.add_argument("action", choices=["list", "approve", "reject", "export"])
    p.add_argument("ids", nargs="*")
    p.add_argument("--status", default="pending", help="pending | approved | auto_approved | rejected | all")
    p.add_argument("--all-kind", help="act on every pending proposal of this kind")
    p.add_argument("--by", default="reviewer")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(func=cmd_proposals)

    p = sub.add_parser("eval", help="score agents vs golden set and rules baseline; promotion gate")
    p.add_argument("--promote", action="store_true", help="record this run as the promoted baseline if it passes")
    p.set_defaults(func=cmd_eval)

    p = sub.add_parser("metrics", help="run, quality, alert and LLM cost report")
    p.set_defaults(func=cmd_metrics)

    p = sub.add_parser("lineage", help="trace one ticket back to its source lines")
    p.add_argument("key")
    p.set_defaults(func=cmd_lineage)

    args = parser.parse_args()
    args.func(Config(), args)


if __name__ == "__main__":
    main()
