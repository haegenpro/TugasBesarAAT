FROM golang:1.27.1-alpine3.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/broker-init ./init

FROM nats:2.15.0-alpine3.22
COPY --from=builder /out/broker-init /usr/local/bin/broker-init
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
