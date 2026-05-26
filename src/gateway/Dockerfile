FROM golang:1.26-alpine AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOOS=linux go build -o /out/gateway ./src/gateway/cmd

FROM alpine:3.20
WORKDIR /app/bin
COPY --from=builder /out/gateway ./gateway
COPY src/gateway/config.yaml /app/config.yaml
ENTRYPOINT ["./gateway"]
