# syntax=docker/dockerfile:1

ARG GO_IMAGE=docker.io/library/golang:1.26.10-bookworm@sha256:ee3901c61e423732605a51f1b8157c849ec01194ea0a274213b5202b2c11e7bf
ARG BASE_IMAGE=gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/aenix-io/threek/internal/agent.Version=${VERSION}" \
      -o /out/threek-agent ./cmd/threek-agent

FROM ${BASE_IMAGE}
ARG VERSION=dev
LABEL org.opencontainers.image.title="threek-agent" \
      org.opencontainers.image.description="threek leader placement agent, run as a sidecar next to etcd" \
      org.opencontainers.image.source="https://github.com/aenix-io/threek" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /out/threek-agent /usr/local/bin/threek-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/threek-agent"]
