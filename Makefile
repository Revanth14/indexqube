GOCACHE ?= $(CURDIR)/.cache/go-build

.PHONY: dev test bench extension-check check

dev:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/gateway

test:
	cd gateway && GOCACHE=$(GOCACHE) go test ./...

bench:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/iqbench

extension-check:
	node --check extension/content.js
	node --check extension/background.js
	node --check extension/popup.js

check: extension-check test bench
