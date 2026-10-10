FROM golang:1.27.1-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/aggregator ./cmd/server

FROM alpine:3.24
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/aggregator /usr/local/bin/aggregator
COPY schemas/seismic.avsc schemas/tsunami.avsc schemas/volcanic-v1.avsc /schemas/
ENTRYPOINT ["/usr/local/bin/aggregator"]
