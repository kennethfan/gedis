GO ?= go
BIN = bin/gedis
IMAGE = gedis:latest

.PHONY: build test vet bench compat docker compose-up compose-down clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/gedis

test:
	$(GO) test -race -shuffle=on -count=1 ./...
	$(MAKE) compat

compat:
	@if command -v redis-server >/dev/null && command -v redis-cli >/dev/null; then \
		./tests/redis-compat/zset.sh; \
		./tests/redis-compat/geo-scan.sh; \
		./tests/redis-compat/bitmap.sh; \
		./tests/redis-compat/hll.sh; \
		./tests/redis-compat/stream-base.sh; \
		./tests/redis-compat/stream-group.sh; \
		./tests/redis-compat/txn.sh; \
		./tests/redis-compat/watch.sh; \
		./tests/redis-compat/pubsub.sh; \
	else \
		echo "compat skipped: redis-server/redis-cli not found"; \
	fi

vet:
	$(GO) vet ./...

bench:
	$(GO) test -run XXX -bench . -benchtime 1000x ./internal/...

docker:
	docker build -t $(IMAGE) .

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v

clean:
	rm -rf bin data
