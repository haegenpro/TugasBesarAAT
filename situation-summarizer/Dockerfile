FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /situation-summarizer .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S app -G app
COPY --from=build /situation-summarizer /usr/local/bin/situation-summarizer
USER app
ENTRYPOINT ["/usr/local/bin/situation-summarizer"]
