COMPOSE = docker compose
RUN = $(COMPOSE) run --rm pipeline python -m medallion
export GIT_SHA := $(shell git describe --always --dirty --abbrev=12 2>/dev/null)

.PHONY: up run ingest silver gold reconcile agents proposals approve reject eval metrics lineage test lint reset demo-incremental

up:            ## start Postgres and build the pipeline image
	$(COMPOSE) build pipeline
	$(COMPOSE) up -d db

run: up        ## full pipeline: ingest -> silver -> gold -> reconcile -> metrics
	$(RUN) run

ingest silver gold reconcile metrics: up
	$(RUN) $@

agents: up     ## run all agents (needs LLM_* settings); writes proposals
	$(RUN) agents

proposals: up  ## list pending agent proposals
	$(RUN) proposals list

approve: up    ## approve proposals: make approve IDS="12 13" BY=devansh
	$(RUN) proposals approve $(IDS) --by $(or $(BY),$(USER))

reject: up     ## reject pending proposals or revoke approvals, then remove matching seeds
	$(RUN) proposals reject $(IDS) --by $(or $(BY),$(USER))

eval: up       ## score agents vs golden set and the rules baseline, apply promotion gate
	$(RUN) eval

lineage: up    ## trace a ticket: make lineage KEY=TKT-3960
	$(RUN) lineage $(KEY)

demo-incremental: up  ## load a second, drifted batch and show the incremental merge
	$(RUN) run --file tests/fixtures/batch2_drift.csv

test: up
	$(COMPOSE) run --rm pipeline pytest -q

lint: up
	$(COMPOSE) run --rm pipeline ruff check src tests

reset:         ## drop all data (volumes) and start clean
	$(COMPOSE) down -v
