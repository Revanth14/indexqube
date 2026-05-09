# syntax=docker/dockerfile:1.7

# --- builder ---------------------------------------------------------
# Pinned major.minor matches go.mod (Go 1.25). Pinning to a digest is
# better for reproducibility -- swap the tag for a sha256 in CI once
# you've chosen a baseline image.
FROM golang:1.25 AS builder

# Build-time arg used to embed a version string. Default 'dev' so the
# image still builds without a CI value.
ARG VERSION=dev

WORKDIR /src

# Module cache layer: dependencies rarely change relative to source.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO_ENABLED=0 + static target so the binary runs on distroless static.
# -trimpath strips local paths; -s -w drops the symbol table.
# main.version is intentionally simple; if you don't have one yet, this
# is a no-op rather than a build failure.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/gateway \
        ./cmd/gateway

# --- runtime ---------------------------------------------------------
# distroless/static:nonroot has no shell, no package manager, no libc.
# Runs as UID 65532 (nonroot). Dramatically reduces attack surface
# compared to alpine or debian:slim.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/gateway /gateway

EXPOSE 8080 9100

USER 65532:65532

ENTRYPOINT ["/gateway"]
