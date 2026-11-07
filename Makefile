# Common tasks. `make help` lists them.
SHELL := /bin/bash
TEST_PG ?= postgres://relay:relay@localhost:55432/relay?sslmode=disable
TEST_REDIS ?= redis://localhost:56379/0
URL ?= http://localhost:8080
LABEL ?= local

.PHONY: help build test test-unit test-deps lint up down logs soak crash chaos load fmt

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -e 's/:.*## /\t/'

build: ## Build api, worker, relayctl into ./bin
	go build -o bin/ ./cmd/...

test-unit: ## Unit tests only (no Postgres/Redis needed)
	go test -short ./...

test-deps: ## Start throwaway Postgres 16 + Redis 7 for integration tests
	docker run -d --rm --name relay-test-pg -e POSTGRES_USER=relay -e POSTGRES_PASSWORD=relay -e POSTGRES_DB=relay -p 55432:5432 postgres:16-alpine
	docker run -d --rm --name relay-test-redis -p 56379:6379 redis:7-alpine

test: ## All tests including integration and e2e crash tests (needs test-deps)
	RELAY_TEST_POSTGRES_URL='$(TEST_PG)' RELAY_TEST_REDIS_URL='$(TEST_REDIS)' go test -race -p 2 -count=1 ./...

lint: ## golangci-lint
	golangci-lint run ./...

fmt: ## gofmt + goimports via golangci-lint
	golangci-lint fmt ./...

up: ## docker compose up the whole stack
	docker compose up -d --build
	@echo "dashboard  http://localhost:8080"
	@echo "grafana    http://localhost:3000"
	@echo "prometheus http://localhost:9090"

down: ## Stop the stack (keeps data volumes)
	docker compose down

logs: ## Tail worker logs
	docker compose logs -f worker

soak: build ## Acceptance: 10,000 flaky jobs at 30%, none stuck
	./bin/relayctl soak -n 10000 -fail-rate 0.3

crash: build ## Acceptance: kill a worker mid-job, its jobs finish elsewhere within 15s
	./bin/relayctl crash

chaos: build ## Acceptance: kill every worker, zero jobs lost
	./bin/relayctl chaos -n 2000

load: ## k6 throughput + latency against URL (default localhost), LABEL = instance type
	loadtest/run.sh $(URL) $(LABEL)
