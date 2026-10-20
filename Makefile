GOLANGCI_LINT ?= golangci-lint

.PHONY: build test lint

build:
	go build -o bin/threek ./cmd/threek

test:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run
