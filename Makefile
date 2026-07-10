.PHONY: build test lint check clean

build:
	go build -o bin/approach ./cmd/approach

test:
	go test -race ./...

lint:
	golangci-lint run

check: build test lint

clean:
	rm -rf bin
