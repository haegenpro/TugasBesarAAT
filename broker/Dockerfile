FROM golang:1.27.1-alpine3.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/broker-init ./init
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/ ./cmd/publish-invalid ./cmd/inspect-dlq

FROM nats:2.15.0-alpine3.22
COPY --from=builder /out/broker-init /usr/local/bin/broker-init
COPY --from=builder /out/publish-invalid /out/inspect-dlq /usr/local/bin/
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
