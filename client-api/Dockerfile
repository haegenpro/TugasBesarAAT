FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /client-api .

FROM alpine:3.24
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /client-api /usr/local/bin/client-api
USER app
ENTRYPOINT ["/usr/local/bin/client-api"]
