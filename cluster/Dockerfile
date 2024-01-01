# docker build -t cursus .
# You need to have setup your .cursusconfig beforehand!
# Setup your initial users, nodes, node read replicas, etc and make sure those are all running before building
# and deploying this image.
FROM golang:1.21.3-bookworm as builder


WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . ./

RUN go build -v -o cursus


FROM debian:bookworm-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/cursus /app/cursus
COPY . /

EXPOSE 7681

CMD ["/app/cursus"]