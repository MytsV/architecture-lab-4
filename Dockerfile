FROM golang:1.14 as build

WORKDIR /go/src/practice-3
COPY . .

RUN CGO_ENABLED=0 go build -o ./server ./cmd/server

FROM alpine:3.11
WORKDIR /opt/practice-3
COPY --from=build /go/src/practice-3/server ./
ENTRYPOINT ["/opt/practice-3/server"]
