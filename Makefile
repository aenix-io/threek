GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)
ETCD_VERSION ?= v3.6.15
IMAGE ?= ghcr.io/aenix-io/threek:$(VERSION)-etcd$(ETCD_VERSION:v%=%)
AGENT_IMAGE ?= ghcr.io/aenix-io/threek-agent:$(VERSION)
LDFLAGS := -X github.com/aenix-io/threek/internal/agent.Version=$(VERSION)

.PHONY: build test lint image agent-image

build:
	go build -ldflags "$(LDFLAGS)" -o bin/threek ./cmd/threek
	go build -ldflags "$(LDFLAGS)" -o bin/threek-agent ./cmd/threek-agent

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

agent-image:
	docker buildx build --load \
		--build-arg VERSION=$(VERSION) \
		--tag $(AGENT_IMAGE) \
		--file Containerfile.agent .
