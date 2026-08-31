GOCACHE ?= $(CURDIR)/.cache/go-build
BIN_DIR ?= $(CURDIR)/bin
GATEWAY_BIN ?= $(BIN_DIR)/indexqube-gateway
IQ_BIN ?= $(BIN_DIR)/iq

.PHONY: dev fmt fmt-check vet test test-race bench alpha-check build build-gateway build-iq check clean

dev:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/gateway

test:
	cd gateway && GOCACHE=$(GOCACHE) go test ./...

test-race:
	cd gateway && GOCACHE=$(GOCACHE) go test -race -count=1 ./...

vet:
	cd gateway && GOCACHE=$(GOCACHE) go vet ./...

fmt:
	cd gateway && gofmt -w .

fmt-check:
	@unformatted="$$(cd gateway && gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		printf 'The following Go files need gofmt:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi

bench:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/iqbench

alpha-check:
	bash scripts/manual_alpha_check.sh

build: build-gateway build-iq

build-gateway:
	mkdir -p $(BIN_DIR)
	cd gateway && GOCACHE=$(GOCACHE) go build -trimpath -o $(GATEWAY_BIN) ./cmd/gateway

build-iq:
	mkdir -p $(BIN_DIR)
	cd gateway && GOCACHE=$(GOCACHE) go build -trimpath -o $(IQ_BIN) ./cmd/iq

check: fmt-check vet test build

clean:
	rm -rf $(BIN_DIR) $(GOCACHE)
