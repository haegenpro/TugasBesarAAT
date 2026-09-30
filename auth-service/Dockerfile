FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /auth-service .

FROM alpine:3.24
RUN addgroup -S app && adduser -S app -G app && mkdir /data && chown app:app /data
COPY --from=build /auth-service /usr/local/bin/auth-service
USER app
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/auth-service"]
