FROM --platform=$BUILDPLATFORM golang:1.20-alpine3.18 AS builder
ARG TARGETOS TARGETARCH

WORKDIR /go/src/kube-rebac-proxy

COPY . /go/src/kube-rebac-proxy

RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build ./cmd/kube-rebac-proxy

FROM cgr.dev/chainguard/static
COPY --from=builder /go/src/kube-rebac-proxy/kube-rebac-proxy /usr/local/bin/
ENTRYPOINT ["kube-rebac-proxy"]
