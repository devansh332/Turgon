import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
SEEDS_DIR = ROOT / "seeds"
ARTIFACTS_DIR = ROOT / "artifacts"
EVALS_DIR = ROOT / "evals"
DEFAULT_SOURCE = ROOT / "data" / "raw_tickets.csv"

SOURCE_COLUMNS = [
    "ticket_id",
    "created_at",
    "resolved_at",
    "category",
    "priority",
    "status",
    "building",
    "description",
    "submitted_by",
    "assigned_to",
    "resolution_notes",
    "cost",
    "sla_hours",
]


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


@dataclass(frozen=True)
class Config:
    database_url: str = field(
        default_factory=lambda: _env("DATABASE_URL", "postgresql://medallion:medallion@localhost:5433/medallion")
    )
    llm_base_url: str = field(default_factory=lambda: _env("LLM_BASE_URL", "https://api.openai.com/v1"))
    llm_api_key: str = field(default_factory=lambda: os.environ.get("LLM_API_KEY", ""))
    llm_model_bulk: str = field(default_factory=lambda: _env("LLM_MODEL_BULK", "gpt-5-mini"))
    llm_model_reasoning: str = field(default_factory=lambda: _env("LLM_MODEL_REASONING", "gpt-5-mini"))
    llm_price_in: float = field(default_factory=lambda: float(_env("LLM_PRICE_INPUT_PER_1M", "0.25")))
    llm_price_out: float = field(default_factory=lambda: float(_env("LLM_PRICE_OUTPUT_PER_1M", "2.00")))
    llm_daily_budget_usd: float = field(default_factory=lambda: float(_env("LLM_DAILY_BUDGET_USD", "2.00")))
    replay_window_hours: int = field(default_factory=lambda: int(_env("REPLAY_WINDOW_HOURS", "48")))
    auto_approve_confidence: float = 0.9


# Alert thresholds (observability). Kept in code so changes go through review.
MAX_QUARANTINE_RATE = 0.02
MAX_NULL_RATE_DELTA = 0.05
MAX_LLM_FAILURE_RATE = 0.10
