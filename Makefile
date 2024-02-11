BUILD_CHANNEL?=local
OS=$(shell uname)
VERSION=v1.12.0
GIT_REVISION = $(shell git rev-parse HEAD | tr -d '\n')
TAG_VERSION?=$(shell git tag --points-at | sort -Vr | head -n1)
CGO_LDFLAGS=""
GO_BUILD_LDFLAGS = -ldflags "-X 'main.Version=${TAG_VERSION}' -X 'main.GitRevision=${GIT_REVISION}'"
TOOL_BIN = bin/gotools/$(shell uname -s)-$(shell uname -m)

app_local_db_url := mongodb://localhost:27017
app_test_db_url := mongodb://localhost:26000/?directConnection=true
app_db_url := $(if $(TEST),$(app_test_db_url),$(app_local_db_url))
app_db_docker_url := mongodb://app-db$(and $(TEST),-test):27017/?directConnection=true

.PHONY: default
default: build-module

.PHONY: tool-install
tool-install:
	GOBIN=`pwd`/$(TOOL_BIN) go install \
		github.com/edaniels/golinters/cmd/combined \
		github.com/golangci/golangci-lint/cmd/golangci-lint \
		github.com/AlekSi/gocov-xml \
		github.com/axw/gocov/gocov \
		gotest.tools/gotestsum \
		github.com/rhysd/actionlint/cmd/actionlint

.PHONY: lint
lint: tool-install
	go mod tidy
	export pkgs="`go list -f '{{.Dir}}' ./... | grep -v internal`" && echo "$$pkgs" | xargs go vet -vettool=$(TOOL_BIN)/combined
	GOGC=50 $(TOOL_BIN)/golangci-lint run -v --fix --config=./golangci.yaml

.PHONY: test
test:
	go test -v -coverprofile=coverage.txt -covermode=atomic ./...

bin/buf bin/protoc-gen-go bin/protoc-gen-grpc-gateway bin/protoc-gen-go-grpc:
	GOBIN=$(shell pwd)/bin go install \
		github.com/bufbuild/buf/cmd/buf \
		google.golang.org/protobuf/cmd/protoc-gen-go \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway \
		google.golang.org/grpc/cmd/protoc-gen-go-grpc

.PHONY: clean
clean: 
	rm -rf bin

.PHONY: initlocal
initlocal:
	docker volume list | grep update-db-vol || docker volume create update-db-vol

.PHONY: down-test-mongo
down-test-mongo:
	docker stop update-test-db && docker rm -v update-test-db || true

.PHONY: up-test-mongo
up-test-mongo: initlocal down-test-mongo
	docker run -d --name update-test-db -v update-db-vol:/data/db -p 27017:27017 ghcr.io/viamrobotics/docker-mongo-rs:6.0

