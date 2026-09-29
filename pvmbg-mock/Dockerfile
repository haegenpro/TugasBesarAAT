FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/pvmbg-mock ./cmd/server

FROM alpine:3.20
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/pvmbg-mock /usr/local/bin/pvmbg-mock
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/pvmbg-mock"]
