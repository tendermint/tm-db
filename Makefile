GOTOOLS = github.com/golangci/golangci-lint/cmd/golangci-lint
PACKAGES=$(shell go list ./...)
INCLUDE = -I=${GOPATH}/src/github.com/tendermint/tm-db -I=${GOPATH}/src -I=${GOPATH}/src/github.com/gogo/protobuf/protobuf

export GO111MODULE = on

all: lint test

### go tests
## By default this will only test memdb, goleveldb, and pebbledb, which do not require cgo
test:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags pebbledb -v

test-cleveldb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags cleveldb -v

test-rocksdb:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags rocksdb -v

test-pebble:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags pebbledb -v


test-all:
	@echo "--> Running go test"
	@go test $(PACKAGES) -tags cleveldb,rocksdb,pebbledb -v

lint:
	@echo "--> Running linter"
	@golangci-lint run
	@go mod verify
.PHONY: lint

format:
	find . -name '*.go' -type f -not -path "*.git*"  -not -name '*.pb.go' -not -name '*pb_test.go' | xargs gofumpt -w -l .
.PHONY: format

