FROM golang:1.27.1-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/pvmbg-mock ./cmd/server

FROM alpine:3.24
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/pvmbg-mock /usr/local/bin/pvmbg-mock
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]
