# docker build -t curode .
# You need to have setup your .curodeconfig beforehand!
# Setup your shared key, read replicas, backup configs, etc.
FROM golang:1.21.3-bookworm as builder


WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . ./

RUN go build -v -o curode


FROM debian:bookworm-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/curode /app/curode
COPY . /

EXPOSE 7682

CMD ["/app/curode"]