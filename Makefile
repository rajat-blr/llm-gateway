REDIS_TEST_ADDR ?= 127.0.0.1:6381
export REDIS_TEST_ADDR

.PHONY: build test test-fuzz check integration demo monitoring down
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

demo:
	docker compose up --build --wait

monitoring:
	docker compose --profile monitoring up --build --wait

down:
	docker compose --profile monitoring down
