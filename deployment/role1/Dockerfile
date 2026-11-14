FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY aggregator/go.mod aggregator/go.sum ./
RUN go mod download
COPY aggregator/ ./
RUN CGO_ENABLED=0 go build -o /out/aggregator ./cmd/server && \
    CGO_ENABLED=0 go build -o /out/raw-ingestion ./cmd/raw-ingestion && \
    CGO_ENABLED=0 go build -o /out/input-cutover ./cmd/input-cutover && \
    CGO_ENABLED=0 go build -o /out/satellite-fixture ./cmd/satellite-fixture && \
    CGO_ENABLED=0 go build -o /out/migrate ./cmd/migrate
FROM alpine:3.24
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /usr/local/bin/
COPY aggregator/schemas/ /schemas/
COPY store/migrations/ /repository/store/migrations/
ENTRYPOINT ["/usr/local/bin/aggregator"]
