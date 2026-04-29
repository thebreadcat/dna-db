FROM rust:1-slim AS builder

WORKDIR /build
COPY engine ./engine
WORKDIR /build/engine
RUN cargo build --release --features http_server --bin dnadb_http

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/engine/target/release/dnadb_http /usr/local/bin/dnadb_http

RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 8787 27017 5432

ENV DNADB_DATA_DIR=/data
ENV DNADB_BIND=0.0.0.0:8787
ENV DNADB_LOG_LEVEL=info
ENV DNADB_MAX_CONNECTIONS=1000

ENTRYPOINT ["dnadb_http"]
CMD ["--data-dir", "/data", "--bind", "0.0.0.0:8787"]
