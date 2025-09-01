FROM --platform=$BUILDPLATFORM golang:1.25.0-alpine3.22 AS builder
ARG TARGETOS TARGETARCH

WORKDIR /go/src/spicedb-kubeapi-proxy

COPY . /go/src/spicedb-kubeapi-proxy

RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build ./cmd/spicedb-kubeapi-proxy

FROM cgr.dev/chainguard/static
COPY --from=builder /go/src/spicedb-kubeapi-proxy/spicedb-kubeapi-proxy /usr/local/bin/
ENTRYPOINT ["spicedb-kubeapi-proxy"]
