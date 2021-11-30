NAME="github.com/odpf/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"

.PHONY: all build clean test

all: build

build:
	go build -ldflags "-X cmd.Version=${VERSION}" ${NAME}

build-dev:
	CGO_ENABLED=0 go build -ldflags "-X cmd.Version=dev" ${NAME}

clean:
	rm -rf dist/

test:
	go test ./... -coverprofile=coverage.out

test-coverage: test
	go tool cover -html=coverage.out

test-e2e:
	go test ./test/e2e -tags=integration -count=1

generate-proto: ## regenerate protos
	@echo " > cloning protobuf from odpf/proton"
	@echo " > generating protobuf"
	@buf generate --template buf.gen.yaml https://github.com/odpf/proton/archive/135a03875fc82b501e686924457a44730fd04268.zip#strip_components=1 --path odpf/assets
	@echo " > protobuf compilation finished"