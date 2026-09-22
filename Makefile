GO ?= go
BIN = bin/redrock
IMAGE = redrock:latest

.PHONY: build test vet bench docker compose-up compose-down clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/redrock

test:
	$(GO) test -race -shuffle=on -count=1 ./...

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
