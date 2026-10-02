FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /consumer .

FROM alpine:3.24
RUN addgroup -S app && adduser -S app -G app && mkdir /data && chown app:app /data
COPY --from=build /consumer /usr/local/bin/consumer
USER app
ENTRYPOINT ["/usr/local/bin/consumer"]
