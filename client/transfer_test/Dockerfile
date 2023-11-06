# Copyright 2020 Alibaba, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# Build the manager binary

FROM golang:1.15 as builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
ENV GOPROXY=https://goproxy.cn
ENV GONOPROXY="*.alibaba-inc.com"
RUN go mod download

# Copy the go source
ADD . .

ARG GOARCH=amd64

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} GO111MODULE=on go build -o transfer cmd/transfer/main.go

FROM busybox:latest
WORKDIR /
COPY --from=builder /workspace/transfer /transfer
COPY --from=builder /workspace/config_example.toml /config_example.toml
COPY --from=builder /workspace/config_example_xa.toml /config_example_xa.toml
COPY --from=builder /workspace/config_example_polarx.toml /config_example_polarx.toml
USER root

ENTRYPOINT ["/transfer"]
