# Thin wrappers. Everything here is plain `docker compose` underneath.
SHELL := /bin/sh
DC    := docker compose
RUN   := $(DC) run --rm --no-deps dispatcher

.PHONY: help up down logs ps seed archive query verify compact retention shell mysql redis reset

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | column -t -s "$$(printf '\t')"

up:        ## build + start the whole stack
	$(DC) up -d --build
	@echo "MinIO console: http://127.0.0.1:$${MINIO_CONSOLE_PORT:-19001}"

down:      ## stop containers, keep data
	$(DC) down

reset:     ## stop and DELETE all data volumes
	$(DC) down -v

ps:        ## container status
	$(DC) ps

logs:      ## follow worker + dispatcher
	$(DC) logs -f worker dispatcher

seed:      ## generate test events (make seed N=200000)
	$(RUN) python -m app.seed --events $${N:-100000}

query:     ## run a report (make query Q=monthly)
	$(RUN) python -m app.query $${Q:-monthly}

verify:    ## manifest vs archive read-back
	$(RUN) python -m app.query verify

compact:   ## merge small parts in every month
	$(RUN) python -m app.compact --all --min-parts 2

retention: ## dry-run MySQL partition cleanup
	$(RUN) python -m app.retention --keep-months $${KEEP:-12}

shell:     ## interactive DuckDB shell over the archive
	$(DC) run --rm duckdb

mysql:     ## mysql client on the hot store
	$(DC) exec mysql sh -c 'mysql -u$$MYSQL_USER -p$$MYSQL_PASSWORD $$MYSQL_DATABASE'

redis:     ## redis-cli on the queue
	$(DC) exec redis sh -c 'redis-cli -a $$REDIS_PASSWORD --no-auth-warning'

tools:     ## start Adminer as well
	$(DC) --profile tools up -d adminer
