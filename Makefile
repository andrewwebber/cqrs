export CGO_ENABLED:=0
export GO111MODULE=on
export GOLANGCI_LINT_FLAGS:=-D gochecknoglobals -D lll
export GOLANGCI_LINT_FLAGS_OVERRIDE:=./.golangci

BUILD_DATE=$(shell date +%Y%m%d-%H:%M:%S)
GROUP=andrewwebber
PROJECTNAME=cqrs

REPO=github.com/$(GROUP)/$(PROJECTNAME)

GOLANGCI_LINT_VERSION="v1.12.5"

all: build

build: bin/$(PROJECTNAME)

.PHONY: bin/$(PROJECTNAME)
bin/$(PROJECTNAME):
	@go build -o bin/$(PROJECTNAME) -v $(REPO)

.PHONY: mod-update
mod-update:
	@go get -u
	@go mod tidy

test: bin/golangci-lint
	@./scripts/test

bin/golangci-lint:
	@curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s $(GOLANGCI_LINT_VERSION)

clean:
	@rm -rf bin


.PHONY: all build clean test
