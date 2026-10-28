REDIS_TEST_ADDR ?= 127.0.0.1:6381
export REDIS_TEST_ADDR

.PHONY: build test test-fuzz check integration demo demo-openai demo-check monitoring baseline release-check release-evidence down
build:
	mkdir -p bin
	go build -trimpath -o bin/gateway ./cmd/gateway
	go build -trimpath -o bin/mock ./cmd/mock

test:
	go test -race -timeout 60s ./...

check: test
	go vet ./...

test-fuzz:
	go test ./internal/gateway -run '^$$' -fuzz FuzzStrictRequest -fuzztime 10s -parallel 2

integration: build
	python3 scripts/sdk_integration.py

demo-check:
	node --test scripts/demo_test.cjs

demo:
	docker compose up --build --wait

demo-openai:
	docker compose -f compose.yaml -f compose.openai.yaml --profile monitoring up --build --wait

baseline: build
	python3 scripts/load_baseline.py

monitoring:
	docker compose --profile monitoring up --build --wait

down:
	docker compose --profile monitoring down

release-check: check build demo-check
	go test ./internal/gateway -run '^$$' -fuzz FuzzStrictJSONEquivalent -fuzztime 10s -parallel 2
	python3 scripts/sdk_integration.py
	python3 scripts/harness_test.py
	python3 scripts/release_load.py --runs 1 --seconds 0.2 --max-concurrency 10 --output tmp/release-load-smoke.json
	python3 scripts/resource_soak.py --seconds 2 --runs 1 --concurrency 4 --slow-rounds 1 --output tmp/resource-soak-smoke.json

release-evidence: build
	python3 scripts/release_load.py
	python3 scripts/resource_soak.py
