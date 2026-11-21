FROM golang:1.27.1-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/aggregator ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/m2-migrate ./cmd/m2-migrate
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/m2-ingestion ./cmd/m2-ingestion
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/m2-worker ./cmd/m2-worker

FROM alpine:3.24
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/aggregator /usr/local/bin/aggregator
COPY --from=builder /out/m2-migrate /usr/local/bin/m2-migrate
COPY --from=builder /out/m2-ingestion /out/m2-worker /usr/local/bin/
COPY schemas/seismic.avsc schemas/tsunami.avsc schemas/volcanic-v1.avsc /schemas/
COPY schemas/volcanic-adapter-v2.avsc /schemas/
ENTRYPOINT ["/usr/local/bin/aggregator"]
