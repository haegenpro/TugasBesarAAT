FROM golang:1.27.1-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/bmkg-mock ./cmd/server

FROM alpine:3.24
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/bmkg-mock /usr/local/bin/bmkg-mock
ENTRYPOINT ["/usr/local/bin/bmkg-mock"]
