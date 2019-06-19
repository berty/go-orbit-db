.PHONY: all
all: test lint

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -v ./...
