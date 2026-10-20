GOLANGCI_LINT ?= golangci-lint
VERSION ?= $(shell git describe --tags --always --dirty)

.PHONY: build test lint

build:
	go build -ldflags "-X github.com/aenix-io/threek/internal/agent.Version=$(VERSION)" -o bin/threek ./cmd/threek

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run
