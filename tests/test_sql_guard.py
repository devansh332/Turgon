import pytest

from medallion.agents.sql_guard import UnsafeSQL, predicate_query, validate_select


@pytest.mark.parametrize(
    "predicate",
    [
        "btrim(cost) IN ('N/A', 'TBD')",
        "created_at ~ '^\\d{4}-\\d{2}-\\d{2}' AND resolved_at IS NOT NULL",
        "lower(btrim(status)) NOT IN ('open', 'closed')",
        "split_part(created_at, '/', 2)::int > 12 AND char_length(created_at) = 10",
        "_extra ? 'channel'",
    ],
)
def test_accepts_read_only_predicates(predicate):
    assert predicate_query(predicate).startswith("SELECT _bronze_id FROM bronze.tickets_raw WHERE")


@pytest.mark.parametrize(
    "predicate, reason",
    [
        ("1=1; DROP TABLE silver.tickets", "exactly one SELECT"),
        ("_bronze_id IN (SELECT _bronze_id FROM silver.tickets)", "table not allowed"),
        ("pg_sleep(10) IS NOT NULL", "function not allowed"),
        ("query_to_xml('select * from meta.llm_cache', true, true, '') IS NOT NULL", "function not allowed"),
        ("pg_ls_dir('.') IS NOT NULL", "function not allowed"),
        ("lo_get(1) IS NOT NULL", "function not allowed"),
        ("password = 'x'", "unknown columns"),
        ("cost = (", "does not parse"),
    ],
)
def test_rejects_unsafe_predicates(predicate, reason):
    with pytest.raises(UnsafeSQL, match=reason):
        predicate_query(predicate)


def test_rejects_writes_even_inside_cte():
    with pytest.raises(UnsafeSQL):
        validate_select("WITH x AS (DELETE FROM bronze.tickets_raw RETURNING *) SELECT * FROM x")


def test_cte_names_cannot_shadow_a_forbidden_table():
    with pytest.raises(UnsafeSQL, match="table not allowed: pg_authid"):
        validate_select("WITH pg_authid AS (SELECT 1) SELECT * FROM pg_authid")
