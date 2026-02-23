# Setup (requires: prek — https://github.com/nicholasgasior/prek; install via `brew install prek` on macOS)
init:
	@command -v prek >/dev/null 2>&1 || { echo "prek not found. Install: brew install prek (macOS) or see https://github.com/nicholasgasior/prek"; exit 1; }
	prek install

# Rust build targets
check:
	cargo check

build:
	cargo build

build-release:
	cargo build --release

test:
	cargo test

test-examples:
	./tests/test_examples.sh

clippy:
	cargo clippy -- -D warnings

fmt:
	cargo fmt --check

lint: fmt clippy
	@echo "All lint checks passed."

docker-build:
	docker build -f docker/Dockerfile -t dbt-temporal .

# Docker Postgres for local examples
PG_URL = postgresql://dev:dev@localhost:5555

postgres-up:
	docker compose -f docker/docker-compose.postgres.yml up -d --wait

postgres-down:
	docker compose -f docker/docker-compose.postgres.yml down

# Seed Postgres databases for local examples
seed-postgres:
	@command -v psql >/dev/null 2>&1 || { echo "psql not found. Install: brew install libpq"; exit 1; }
	psql $(PG_URL)/single_project -f examples/single-project/seed-postgres.sql
	psql $(PG_URL)/multi_project -f examples/multi-project/seed-postgres.sql
	psql $(PG_URL)/env_override -f examples/env-override/seed-postgres.sql
	@echo "Seeded Postgres databases."

# Local dev environment (requires: temporal CLI)
dev:
	@echo "Starting Temporal dev server..."
	@echo "  gRPC:  localhost:7233"
	@echo "  UI:    http://localhost:8233"
	@echo "  Press Ctrl+C to shutdown"
	temporal server start-dev

# Resolve GOOGLE_CLOUD_PROJECT: explicit env var > gcloud config
GCP_PROJECT = $${GOOGLE_CLOUD_PROJECT:-$$(gcloud config get-value project 2>/dev/null)}

# Example workers (each on its own task queue)
# BigQuery example requires GOOGLE_CLOUD_PROJECT (or gcloud default) and application default credentials.
# Uses `cargo build` + `exec` so Ctrl+C cleanly kills the worker (no orphaned processes).
run-worker-single: build
	TEMPORAL_TASK_QUEUE=single-project \
	DBT_PROJECT_DIR=examples/single-project \
	exec ./target/debug/dbt-temporal

run-worker-multi: build
	TEMPORAL_TASK_QUEUE=multi-project \
	DBT_PROJECTS_DIR=examples/multi-project \
	exec ./target/debug/dbt-temporal

run-worker-env: build
	TEMPORAL_TASK_QUEUE=env-override \
	DBT_PROJECT_DIR=examples/env-override \
	exec ./target/debug/dbt-temporal

run-worker-jaffle-shop: build
	TEMPORAL_TASK_QUEUE=jaffle-shop \
	DBT_PROJECT_DIRS=git+https://github.com/dbt-labs/jaffle-shop-classic.git#main \
	DBT_PROFILES_DIR=examples/jaffle-shop \
	exec ./target/debug/dbt-temporal

run-worker-bigquery: build
	TEMPORAL_TASK_QUEUE=bigquery GOOGLE_CLOUD_PROJECT=$(GCP_PROJECT) \
	DBT_PROJECT_DIR=examples/bigquery \
	exec ./target/debug/dbt-temporal

# Submit workflows to example workers
submit-all: submit-workflow-single submit-workflow-multi submit-workflow-env submit-workflow-jaffle-shop

submit-workflow-single:
	temporal workflow start \
		--type dbt_run \
		--task-queue single-project \
		--workflow-id dbt_test_project \
		--id-conflict-policy TerminateExisting \
		--input '{}'

submit-workflow-multi:
	temporal workflow start \
		--type dbt_run \
		--task-queue multi-project \
		--workflow-id analytics \
		--id-conflict-policy TerminateExisting \
		--input '{"project": "analytics"}'
	temporal workflow start \
		--type dbt_run \
		--task-queue multi-project \
		--workflow-id marketing \
		--id-conflict-policy TerminateExisting \
		--input '{"project": "marketing"}'

submit-workflow-multi-analytics:
	temporal workflow start \
		--type dbt_run \
		--task-queue multi-project \
		--workflow-id analytics \
		--id-conflict-policy TerminateExisting \
		--input '{"project": "analytics"}'

submit-workflow-multi-marketing:
	temporal workflow start \
		--type dbt_run \
		--task-queue multi-project \
		--workflow-id marketing \
		--id-conflict-policy TerminateExisting \
		--input '{"project": "marketing"}'

submit-workflow-env:
	temporal workflow start \
		--type dbt_run \
		--task-queue env-override \
		--workflow-id env_override_demo \
		--id-conflict-policy TerminateExisting \
		--input '{}'

submit-workflow-jaffle-shop:
	temporal workflow start \
		--type dbt_run \
		--task-queue jaffle-shop \
		--workflow-id jaffle_shop \
		--id-conflict-policy TerminateExisting \
		--input '{}'

submit-workflow-bigquery:
	temporal workflow start \
		--type dbt_run \
		--task-queue bigquery \
		--workflow-id bigquery_example \
		--id-conflict-policy TerminateExisting \
		--input '{}'

# Run all examples: start Postgres, seed, start Temporal, build, start workers, submit workflows.
# BigQuery example requires GCP credentials (skipped if unavailable).
# Ctrl+C kills workers and Temporal; run `make cleanup-db` to reset the Temporal database.
DIM    := \033[2m
BOLD   := \033[1m
GREEN  := \033[32m
CYAN   := \033[36m
YELLOW := \033[33m
RESET  := \033[0m

run-examples:
	@G="$(GREEN)"; C="$(CYAN)"; Y="$(YELLOW)"; B="$(BOLD)"; D="$(DIM)"; R="$(RESET)"; \
	ok="$${G}ok$${R}"; fail="$${Y}FAILED$${R}"; \
	printf "$${D}[1/5]$${R} Starting Postgres..."; \
	$(MAKE) --no-print-directory postgres-up > /dev/null 2>&1 && printf " $${ok}\n" || { printf " $${fail}. Is Docker running?\n"; exit 1; }; \
	printf "$${D}[2/5]$${R} Seeding Postgres databases..."; \
	$(MAKE) --no-print-directory seed-postgres > /dev/null 2>&1 && printf " $${ok}\n" || { printf " $${fail}. Is psql installed?\n"; exit 1; }; \
	printf "$${D}[3/5]$${R} Building..."; \
	cargo build > /dev/null 2>&1 && printf " $${ok}\n" || { printf " $${fail}. Run 'cargo build' to see errors.\n"; exit 1; }; \
	trap 'trap - INT TERM; printf "\n$${D}Shutting down...$${R}"; kill 0; wait 2>/dev/null; printf " done.\n$${D}Run '\''make cleanup-db'\'' to reset the Temporal database.$${R}\n$${D}Run '\''make postgres-down'\'' to stop Postgres.$${R}\n"; exit 0' INT TERM; \
	printf "$${D}[4/5]$${R} Starting Temporal server..."; \
	temporal server start-dev > /dev/null 2>&1 & \
	sleep 2; \
	temporal workflow list > /dev/null 2>&1 && printf " $${ok}\n" || { printf " $${fail}. Is the temporal CLI installed?\n"; kill 0; wait 2>/dev/null; exit 1; }; \
	printf "$${D}[5/5]$${R} Starting workers..."; \
	w_errors=$$(mktemp); w_total=4; \
	TEMPORAL_TASK_QUEUE=single-project DBT_PROJECT_DIR=examples/single-project \
		./target/debug/dbt-temporal > /dev/null 2>>"$$w_errors" & p1=$$!; \
	TEMPORAL_TASK_QUEUE=multi-project DBT_PROJECTS_DIR=examples/multi-project \
		./target/debug/dbt-temporal > /dev/null 2>>"$$w_errors" & p2=$$!; \
	TEMPORAL_TASK_QUEUE=env-override DBT_PROJECT_DIR=examples/env-override \
		./target/debug/dbt-temporal > /dev/null 2>>"$$w_errors" & p3=$$!; \
	TEMPORAL_TASK_QUEUE=jaffle-shop \
		DBT_PROJECT_DIRS=git+https://github.com/dbt-labs/jaffle-shop-classic.git#main \
		DBT_PROFILES_DIR=examples/jaffle-shop \
		./target/debug/dbt-temporal > /dev/null 2>>"$$w_errors" & p4=$$!; \
	GCP=$${GOOGLE_CLOUD_PROJECT:-$$(gcloud config get-value project 2>/dev/null)}; \
	if [ -n "$$GCP" ]; then \
		w_total=5; \
		GOOGLE_CLOUD_PROJECT="$$GCP" TEMPORAL_TASK_QUEUE=bigquery DBT_PROJECT_DIR=examples/bigquery \
			./target/debug/dbt-temporal > /dev/null 2>>"$$w_errors" & p5=$$!; \
	else \
		printf "\n  $${D}(skipping BigQuery worker — no GCP credentials)$${R}\n  "; \
	fi; \
	sleep 5; \
	w_alive=0; kill -0 $$p1 2>/dev/null && w_alive=$$((w_alive+1)); \
	kill -0 $$p2 2>/dev/null && w_alive=$$((w_alive+1)); \
	kill -0 $$p3 2>/dev/null && w_alive=$$((w_alive+1)); \
	kill -0 $$p4 2>/dev/null && w_alive=$$((w_alive+1)); \
	[ -n "$$p5" ] && kill -0 $$p5 2>/dev/null && w_alive=$$((w_alive+1)); \
	if [ $$w_alive -eq $$w_total ]; then printf " $${ok} ($$w_alive/$$w_total)\n"; \
	elif [ $$w_alive -gt 0 ]; then printf " $${Y}$$w_alive/$$w_total started$${R}\n"; \
	else printf " $${fail}. Check errors:\n"; sed 's/^/  /' "$$w_errors" | head -5; rm -f "$$w_errors"; kill 0; wait 2>/dev/null; exit 1; fi; \
	rm -f "$$w_errors"; \
	printf "Submitting workflows..."; \
	$(MAKE) --no-print-directory submit-all > /dev/null 2>&1; \
	if [ -n "$$GCP" ]; then \
		$(MAKE) --no-print-directory submit-workflow-bigquery > /dev/null 2>&1; \
	fi; \
	printf " $${ok}\n"; \
	printf "\n"; \
	L="$${B}  │$${R}"; E="$${B}│$${R}"; \
	printf "$${B}  ┌──────────────────────────────────────────────┐$${R}\n"; \
	printf "$$L  dbt-temporal examples                       $$E\n"; \
	printf "$${B}  ├──────────────────────────────────────────────┤$${R}\n"; \
	printf "$$L                                              $$E\n"; \
	printf "$$L  Temporal UI:  $${C}http://localhost:8233$${R}         $$E\n"; \
	printf "$$L  Backend:      $${C}Postgres (Docker)$${R}            $$E\n"; \
	if [ -n "$$GCP" ]; then \
	printf "$$L  GCP Project:  $${C}$$GCP$${R}"; \
	pad=$$((28 - $${#GCP})); [ $$pad -gt 0 ] && printf "%*s" $$pad ""; printf " $$E\n"; \
	fi; \
	printf "$$L                                              $$E\n"; \
	printf "$$L  $${D}Submit more workflows:$${R}                      $$E\n"; \
	printf "$$L    $${C}make submit-workflow-single$${R}               $$E\n"; \
	printf "$$L    $${C}make submit-workflow-multi-analytics$${R}      $$E\n"; \
	printf "$$L    $${C}make submit-workflow-multi-marketing$${R}      $$E\n"; \
	printf "$$L    $${C}make submit-workflow-env$${R}                  $$E\n"; \
	printf "$$L    $${C}make submit-workflow-jaffle-shop$${R}          $$E\n"; \
	if [ -n "$$GCP" ]; then \
	printf "$$L    $${C}make submit-workflow-bigquery$${R}             $$E\n"; \
	fi; \
	printf "$$L                                              $$E\n"; \
	printf "$$L  $${D}Press Ctrl+C to stop.$${R}                       $$E\n"; \
	printf "$${B}  └──────────────────────────────────────────────┘$${R}\n"; \
	printf "\n"; \
	open http://localhost:8233; \
	wait

# Kill any running workers or Temporal dev server
cleanup:
	-pkill -f 'target/debug/dbt-temporal' 2>/dev/null
	-pkill -f 'target/release/dbt-temporal' 2>/dev/null
	-pkill -f 'temporal server start-dev' 2>/dev/null
	@echo "Cleaned up."

# Reset the Temporal dev server database
cleanup-db:
	@rm -rf ~/.config/temporalio/data 2>/dev/null; echo "Temporal database reset."

# Open the Temporal UI
ui:
	open http://localhost:8233
