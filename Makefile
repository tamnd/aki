# Build into bin/ (gitignored) so the binary never collides with package source.
BINARY  := bin/f1srv
PKG     := ./cmd/f1srv
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS := -s -w -X main.Version=$(VERSION) -X main.Commit=$(COMMIT)

.PHONY: build install test race vet fmt lint tidy clean run

build:
	@mkdir -p $(dir $(BINARY))
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) $(PKG)

install:
	CGO_ENABLED=0 go install -trimpath -ldflags "$(LDFLAGS)" $(PKG)

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w -s .

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin dist

run: build
	./$(BINARY) $(ARGS)
