FROM golang:1.21-alpine AS build
RUN apk add --no-cache bash
WORKDIR /go/src/proglog
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/Proglog
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.36 && \
    GHP_ARCH=linux-arm64 && \
    wget -qO/go/bin/grpc_health_probe \
    https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-${GHP_ARCH} && \
    chmod +x /go/bin/grpc_health_probe
FROM scratch
RUN apk add --no-cache bash
COPY --from=build /go/bin/proglog /bin/proglog
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/proglog"]
