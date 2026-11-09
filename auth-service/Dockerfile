FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /auth-service .

FROM alpine:3.24
RUN addgroup -S -g 10001 app && adduser -S -u 10001 app -G app && mkdir /data && chown app:app /data
COPY --from=build /auth-service /usr/local/bin/auth-service
USER app
ENTRYPOINT ["/usr/local/bin/auth-service"]
