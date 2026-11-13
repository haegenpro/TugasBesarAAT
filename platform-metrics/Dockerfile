FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /metrics .
FROM alpine:3.24
COPY --from=build /metrics /usr/local/bin/platform-metrics
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/platform-metrics"]
