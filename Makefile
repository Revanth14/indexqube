GOCACHE ?= $(CURDIR)/.cache/go-build
BIN_DIR ?= $(CURDIR)/bin
DIST_DIR ?= $(CURDIR)/dist
GATEWAY_BIN ?= $(BIN_DIR)/indexqube-gateway
EXTENSION_ZIP ?= $(DIST_DIR)/indexqube-extension.zip

.PHONY: dev test bench alpha-check extension-check build-gateway package-extension release-local check

dev:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/gateway

test:
	cd gateway && GOCACHE=$(GOCACHE) go test ./...

bench:
	cd gateway && GOCACHE=$(GOCACHE) go run ./cmd/iqbench

alpha-check:
	bash scripts/manual_alpha_check.sh

extension-check:
	node --check extension/content.js
	node --check extension/background.js
	node --check extension/popup.js
	node --check extension/options.js

build-gateway:
	mkdir -p $(BIN_DIR)
	cd gateway && GOCACHE=$(GOCACHE) go build -trimpath -o $(GATEWAY_BIN) ./cmd/gateway

package-extension: extension-check
	EXTENSION_ZIP=$(EXTENSION_ZIP) bash scripts/package_extension.sh

release-local: build-gateway package-extension
	@printf 'Built %s\nPackaged %s\n' "$(GATEWAY_BIN)" "$(EXTENSION_ZIP)"

check: extension-check test bench
