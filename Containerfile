# syntax=docker/dockerfile:1

ARG GO_IMAGE=docker.io/library/golang:1.26.10-bookworm@sha256:ee3901c61e423732605a51f1b8157c849ec01194ea0a274213b5202b2c11e7bf
# The same base as the upstream etcd image, so the result differs from it only in /usr/local/bin.
ARG BASE_IMAGE=gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS threek
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
      -o /out/etcd ./cmd/threek

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS etcd
ARG TARGETARCH
ARG ETCD_VERSION
WORKDIR /download
COPY build/etcd.sha256 .
RUN set -eu; \
    archive="etcd-${ETCD_VERSION}-linux-${TARGETARCH}.tar.gz"; \
    curl --fail --silent --show-error --location --output "${archive}" \
      "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/${archive}"; \
    grep "  ${archive}\$" etcd.sha256 | sha256sum --check --strict; \
    tar --extract --gzip --file "${archive}"; \
    mkdir /out; \
    dir="etcd-${ETCD_VERSION}-linux-${TARGETARCH}"; \
    mv "${dir}/etcd" /out/etcd-upstream; \
    mv "${dir}/etcdctl" "${dir}/etcdutl" /out/

FROM ${BASE_IMAGE}
ARG VERSION=dev
ARG ETCD_VERSION
LABEL org.opencontainers.image.title="threek" \
      org.opencontainers.image.description="etcd ${ETCD_VERSION} with the threek leader placement supervisor" \
      org.opencontainers.image.source="https://github.com/aenix-io/threek" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=etcd /out/ /usr/local/bin/
COPY --from=threek /out/etcd /usr/local/bin/etcd
WORKDIR /var/etcd/
WORKDIR /var/lib/etcd/
EXPOSE 2379 2380
CMD ["/usr/local/bin/etcd"]
