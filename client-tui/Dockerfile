FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /client-tui .

FROM alpine:3.24
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /client-tui /usr/local/bin/client-tui
USER app
ENTRYPOINT ["/usr/local/bin/client-tui"]
