REPO ?=
RAW_VERSION = $(strip $(shell cat __version__))
GIT_COMMIT := $(shell git rev-parse --short HEAD)
BUILD_TIME := $(shell date '+%Y%m%d%H%M%S')
VERSION := ${RAW_VERSION}-${BUILD_TIME}-${GIT_COMMIT}
IMG_TAG := ${GIT_COMMIT}-${BUILD_TIME}
IMG := $(if ${REPO},${REPO}/,)polardbx-transfer-test:${IMG_TAG}

INJECT_VERSION := -ldflags "-X transfer/pkg/version.V=${VERSION}"

all: build

build-local:
	mkdir -p target
	env GOPROXY=https://goproxy.cn go build ${INJECT_VERSION} -o target/transfer ./cmd/transfer

build:
	mkdir -p target
	env GOPROXY=https://goproxy.cn GOOS=linux GOARCH=amd64 go build ${INJECT_VERSION} -o target/transfer ./cmd/transfer

run-local: build-local
	./target/transfer

run: build
	./target/transfer

clean:
	rm -r target

docker-push: docker-build
	docker push ${IMG}

docker-build:
	docker build -t ${IMG} .

docker-build-arm:
	docker buildx build -t ${IMG}-arm64 --platform linux/arm64 --build-arg GOARCH=arm64 -o type=docker .
