"""One small client for every agent: OpenAI-compatible (OpenAI, Ollama, vLLM...), JSON-only output validated
by pydantic with a repair retry, a content-addressed cache in Postgres, cost logging and a daily budget."""

import hashlib
import json
import re
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TypeVar

from openai import OpenAI, OpenAIError
from pydantic import BaseModel, ValidationError

from medallion.config import MAX_LLM_FAILURE_RATE, Config
from medallion.db import connect
from medallion.log import event

PROMPTS_DIR = Path(__file__).parent / "prompts"
T = TypeVar("T", bound=BaseModel)


class LLMError(RuntimeError):
    pass


class BudgetExceeded(LLMError):
    pass


def load_prompt(name: str) -> tuple[str, str]:
    text = (PROMPTS_DIR / f"{name}.md").read_text()
    m = re.match(r"<!--\s*version:\s*(\S+)\s*-->\n", text)
    if not m:
        raise ValueError(f"prompt {name} has no version header")
    return m.group(1), text[m.end() :]


def batched(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class LLMClient:
    def __init__(self, cfg: Config):
        if not cfg.llm_api_key:
            raise LLMError("LLM_API_KEY is not set; agents need an LLM (see .env.example)")
        self.cfg = cfg
        self.client = OpenAI(base_url=cfg.llm_base_url, api_key=cfg.llm_api_key, timeout=600, max_retries=2)
        self.log_conn = connect(cfg, autocommit=True)  # bookkeeping survives an agent's rollback

    def prompt_version(self, name: str) -> str:
        return load_prompt(name)[0]

    def cost(self, tokens_in: int, tokens_out: int) -> float:
        return (tokens_in * self.cfg.llm_price_in + tokens_out * self.cfg.llm_price_out) / 1_000_000

    def _alert(self, severity: str, check: str, message: str, value, threshold) -> None:
        self.log_conn.execute(
            "INSERT INTO meta.alerts (severity, check_name, message, value, threshold) VALUES (%s,%s,%s,%s,%s)",
            (severity, check, message, value, threshold),
        )

    def _check_budget(self, pending_usd: float = 0.0) -> None:
        """Spend logged today plus what the current call has already used must stay under the budget."""
        spent = self.log_conn.execute(
            "SELECT coalesce(sum(cost_usd), 0) AS s FROM meta.llm_calls WHERE created_at >= current_date"
        ).fetchone()["s"]
        total = float(spent) + pending_usd
        if total >= self.cfg.llm_daily_budget_usd:
            self._alert(
                "error",
                "llm_budget",
                "daily LLM budget exhausted; agent calls refused",
                total,
                self.cfg.llm_daily_budget_usd,
            )
            raise BudgetExceeded(f"daily LLM budget ${self.cfg.llm_daily_budget_usd} reached (${total:.4f})")

    def check_failure_rate(self, agent: str) -> float:
        """Share of today's non-cached calls for this agent that ended without valid output."""
        row = self.log_conn.execute(
            """SELECT count(*) AS n, count(*) FILTER (WHERE NOT ok) AS failed FROM meta.llm_calls
               WHERE agent = %s AND NOT cache_hit AND created_at >= current_date""",
            (agent,),
        ).fetchone()
        rate = row["failed"] / row["n"] if row["n"] else 0.0
        if rate > MAX_LLM_FAILURE_RATE:
            self._alert(
                "warning",
                "llm_failure_rate",
                f"{agent}: {row['failed']}/{row['n']} calls failed today",
                rate,
                MAX_LLM_FAILURE_RATE,
            )
        return rate

    def cached_items(self, prompt_name: str, model: str, keys: list[str], schema: type[T]) -> dict[str, T]:
        """Previously validated per-item results for this prompt version and model."""
        rows = self.log_conn.execute(
            """SELECT item_key, response FROM meta.llm_item_cache
               WHERE prompt_name=%s AND prompt_version=%s AND model=%s AND item_key = ANY(%s)""",
            (prompt_name, self.prompt_version(prompt_name), model, keys),
        ).fetchall()
        out: dict[str, T] = {}
        for r in rows:
            try:
                out[r["item_key"]] = schema.model_validate(r["response"])
            except ValidationError:
                continue  # schema tightened since it was cached: ask again
        return out

    def cache_items(self, prompt_name: str, model: str, items: dict[str, BaseModel]) -> None:
        version = self.prompt_version(prompt_name)
        for key, result in items.items():
            self.log_conn.execute(
                """INSERT INTO meta.llm_item_cache (prompt_name, prompt_version, model, item_key, response)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (prompt_name, prompt_version, model, item_key)
                   DO UPDATE SET response = EXCLUDED.response""",
                (prompt_name, version, model, key, result.model_dump_json()),
            )

    def _log_call(
        self, agent, prompt_name, version, model, *, cache_hit, ok, attempts=0, tokens=(0, 0), latency_ms=0, error=None
    ) -> None:
        self.log_conn.execute(
            """INSERT INTO meta.llm_calls (agent, prompt_name, prompt_version, model, cache_hit, ok, attempts,
                 input_tokens, output_tokens, cost_usd, latency_ms, error)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                agent,
                prompt_name,
                version,
                model,
                cache_hit,
                ok,
                attempts,
                tokens[0],
                tokens[1],
                self.cost(*tokens),
                latency_ms,
                error,
            ),
        )

    def structured(
        self,
        *,
        agent: str,
        prompt_name: str,
        payload: dict,
        schema: type[T],
        model: str | None = None,
        max_attempts: int = 3,
        use_cache: bool = True,
    ) -> T:
        version, system = load_prompt(prompt_name)
        model = model or self.cfg.llm_model_bulk
        user = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        key = hashlib.sha256(f"{prompt_name}|{version}|{model}|{user}".encode()).hexdigest()

        if use_cache:
            hit = self.log_conn.execute("SELECT response FROM meta.llm_cache WHERE cache_key=%s", (key,)).fetchone()
            if hit:
                try:
                    result = schema.model_validate(hit["response"])
                except ValidationError:
                    event("llm_cache_stale", agent=agent, prompt=prompt_name)  # schema changed since cached
                else:
                    self._log_call(agent, prompt_name, version, model, cache_hit=True, ok=True)
                    return result

        messages = [
            {
                "role": "system",
                "content": system.rstrip()
                + "\n\n## Output\nReturn one JSON object and nothing else. It must validate against this JSON Schema:\n"
                + json.dumps(schema.model_json_schema()),
            },
            {"role": "user", "content": user},
        ]
        tokens_in = tokens_out = 0
        started = time.monotonic()
        error = None
        attempt = 0
        try:
            for attempt in range(1, max_attempts + 1):
                self._check_budget(self.cost(tokens_in, tokens_out))  # repair retries are paid calls too
                resp = self.client.chat.completions.create(
                    model=model, messages=messages, response_format={"type": "json_object"}
                )
                if resp.usage:
                    tokens_in += resp.usage.prompt_tokens
                    tokens_out += resp.usage.completion_tokens
                text = resp.choices[0].message.content or ""
                try:
                    result = schema.model_validate_json(text)
                except ValidationError as exc:
                    error = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()[:10])
                    event("llm_validation_failed", agent=agent, prompt=prompt_name, attempt=attempt, error=error)
                    repair = f"That output failed validation: {error}. Return the corrected JSON object only."
                    messages += [{"role": "assistant", "content": text}, {"role": "user", "content": repair}]
                    continue
                latency = int((time.monotonic() - started) * 1000)
                self.log_conn.execute(
                    """INSERT INTO meta.llm_cache (cache_key, agent, prompt_name, prompt_version, model, response)
                       VALUES (%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (cache_key) DO UPDATE SET response = EXCLUDED.response, created_at = now()""",
                    (key, agent, prompt_name, version, model, result.model_dump_json()),
                )
                self._log_call(
                    agent,
                    prompt_name,
                    version,
                    model,
                    cache_hit=False,
                    ok=True,
                    attempts=attempt,
                    tokens=(tokens_in, tokens_out),
                    latency_ms=latency,
                )
                event(
                    "llm_call",
                    agent=agent,
                    prompt=prompt_name,
                    model=model,
                    attempts=attempt,
                    tokens_in=tokens_in,
                    tokens_out=tokens_out,
                    latency_ms=latency,
                    cost_usd=round(self.cost(tokens_in, tokens_out), 5),
                )
                return result
        except (OpenAIError, BudgetExceeded) as exc:
            error = f"{type(exc).__name__}: {str(exc)[:300]}"
            self._log_call(
                agent,
                prompt_name,
                version,
                model,
                cache_hit=False,
                ok=False,
                attempts=attempt,
                tokens=(tokens_in, tokens_out),
                latency_ms=int((time.monotonic() - started) * 1000),
                error=error,
            )
            self.check_failure_rate(agent)
            raise

        self._log_call(
            agent,
            prompt_name,
            version,
            model,
            cache_hit=False,
            ok=False,
            attempts=max_attempts,
            tokens=(tokens_in, tokens_out),
            latency_ms=int((time.monotonic() - started) * 1000),
            error=error,
        )
        self.check_failure_rate(agent)
        raise LLMError(f"{prompt_name}: no valid output after {max_attempts} attempts ({error})")
