# Multi-stage build producing a single small image that contains all three
# StorageMesh binaries (gateway, storagenode, worker). Compose / Kubernetes pick
# which one to run via the container command. One image keeps the build simple
# and guarantees all components are built from the identical commit.

# ---- build stage ----
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache module downloads: copy manifests first so `go mod download` is only
# re-run when dependencies change, not on every source edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO disabled => a static binary that runs in a scratch/alpine image with no
# libc. Trim debug info for size.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/gateway     ./cmd/gateway  && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/storagenode ./cmd/storagenode && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/worker      ./cmd/worker

# ---- runtime stage ----
FROM alpine:3.20
# ca-certificates for any outbound TLS; wget for container healthchecks.
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/gateway /out/storagenode /out/worker /usr/local/bin/
# Default command; overridden per service in compose/k8s.
CMD ["gateway"]
