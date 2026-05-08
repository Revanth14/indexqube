GOCACHE ?= $(CURDIR)/.cache/go-build
BIN_DIR ?= $(CURDIR)/bin
DIST_DIR ?= $(CURDIR)/dist
GATEWAY_BIN ?= $(BIN_DIR)/indexqube-gateway
EXTENSION_ZIP ?= $(DIST_DIR)/indexqube-extension.zip
VSIX_OUT ?= $(CURDIR)/vscode-extension/dist

.PHONY: dev test bench alpha-check extension-check build-gateway package-extension package-vsix release-vsix release-local check validate-vsix

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

package-vsix:
	cd vscode-extension && npm run package:vsix

validate-vsix:
	bash scripts/validate_vsix.sh

release-vsix: build-gateway package-vsix
	@printf 'Gateway: %s\nVSIX:    %s\n' "$(GATEWAY_BIN)" "$$(ls $(VSIX_OUT)/*.vsix 2>/dev/null | head -1)"

release-local: build-gateway package-extension
	@printf 'Built %s\nPackaged %s\n' "$(GATEWAY_BIN)" "$(EXTENSION_ZIP)"

check: extension-check test bench
