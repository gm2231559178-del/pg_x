FROM rust:1.94-slim-bookworm AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev perl  build-essential cmake musl-tools \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY . .

RUN rustup target add x86_64-unknown-linux-musl
ENV CC_x86_64_unknown_linux_musl=musl-gcc
RUN cargo build --release --all-features  --target=x86_64-unknown-linux-musl

FROM alpine AS runtime

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/pgx /usr/bin/

ENTRYPOINT ["pgx"]
