GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)
IMAGE ?= ghcr.io/aenix-io/threek-agent:$(VERSION)

.PHONY: build test lint image

build:
	go build -ldflags "-X github.com/aenix-io/threek/internal/agent.Version=$(VERSION)" -o bin/threek-agent ./cmd/threek-agent

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run

image:
	docker buildx build --load \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE) \
		--file Containerfile .
