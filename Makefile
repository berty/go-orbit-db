.PHONY: all
all: test lint

.PHONY: lint
lint:
	golangci-lint run --verbose ./...

.PHONY: test
test:
	go test -cover -coverprofile=coverage.txt -covermode=atomic -v ./...
