GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)
ETCD_VERSION ?= v3.6.15
IMAGE ?= ghcr.io/aenix-io/threek:$(VERSION)-etcd$(ETCD_VERSION:v%=%)

.PHONY: build test lint image

build:
	go build -ldflags "-X github.com/aenix-io/threek/internal/agent.Version=$(VERSION)" -o bin/threek ./cmd/threek

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run

image:
	docker buildx build --load \
		--build-arg VERSION=$(VERSION) \
		--build-arg ETCD_VERSION=$(ETCD_VERSION) \
		--tag $(IMAGE) \
		--file Containerfile .
