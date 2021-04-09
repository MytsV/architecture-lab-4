FROM golang:1.15 as build

RUN apt-get update && apt-get install -y ninja-build

# TODO: Змініть на власну реалізацію системи збірки
RUN go get -u github.com/roman-mazur/bood/cmd/bood

WORKDIR /go/src/practice-2
COPY . .

RUN CGO_ENABLED=0 bood

# ==== Final image ====
FROM alpine:3.11
WORKDIR /opt/practice-2
COPY entry.sh ./
COPY --from=build /go/src/practice-2/out/bin/* ./
ENTRYPOINT ["/opt/practice-2/entry.sh"]
CMD ["server"]
