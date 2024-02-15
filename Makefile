GOTOOLS = github.com/golangci/golangci-lint/cmd/golangci-lint
PACKAGES=$(shell go list ./...)
INCLUDE = -I=${GOPATH}/src/github.com/cometbft/cometbft-db -I=${GOPATH}/src -I=${GOPATH}/src/github.com/gogo/protobuf/protobuf
DOCKER_TEST_IMAGE ?= cometbft/cometbft-db-testing
DOCKER_TEST_IMAGE_VERSION ?= latest
NON_INTERACTIVE ?= 0
DOCKER_TEST_INTERACTIVE_FLAGS ?= -it

ifeq (1,$(NON_INTERACTIVE))
	DOCKER_TEST_INTERACTIVE_FLAGS :=
endif

all: lint test

### go tests
## By default this will only test memdb & goleveldb
test:
	@echo "--> Running go test"
	@go test $(PACKAGES) -v
.PHONY: test


test-all-with-coverage:
	@echo "--> Running go test for all databases, with coverage"
	@CGO_ENABLED=1 go test ./... \
		-mod=readonly \
		-timeout 8m \
		-race \
		-coverprofile=coverage.txt \
		-covermode=atomic \
		-v
.PHONY: test-all-with-coverage

lint:
	@echo "--> Running linter"
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run
	@go mod verify
.PHONY: lint

format:
	find . -name '*.go' -type f -not -path "*.git*" -not -name '*.pb.go' -not -name '*pb_test.go' | xargs gofmt -w -s
	find . -name '*.go' -type f -not -path "*.git*"  -not -name '*.pb.go' -not -name '*pb_test.go' | xargs goimports -w
.PHONY: format

docker-test-image:
	@echo "--> Building Docker test image"
	@cd tools && \
		docker build -t $(DOCKER_TEST_IMAGE):$(DOCKER_TEST_IMAGE_VERSION) .
.PHONY: docker-test-image

# Runs the same test as is executed in CI, but locally.


tools:
	go get -v $(GOTOOLS)
.PHONY: tools

vulncheck:
		@go run golang.org/x/vuln/cmd/govulncheck@latest ./...
.PHONY: vulncheck
