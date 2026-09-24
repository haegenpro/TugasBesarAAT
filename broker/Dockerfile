FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 \
    GOOS=linux \
    go build \
    -trimpath \
    -o /out/broker-init \
    ./init


FROM nats:2.15-alpine

COPY --from=builder \
    /out/broker-init \
    /usr/local/bin/broker-init

COPY nats.conf /etc/nats/nats.conf
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
