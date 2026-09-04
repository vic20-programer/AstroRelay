# Build stage
FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# Runtime stage — slim, no compiler in the final image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/astro-relay /usr/local/bin/astro-relay
# Render sets $PORT at runtime; main.rs reads it (falls back to 7878 locally).
# Northflank doesn't inject $PORT the way Render does — it instead reads this
# EXPOSE line to auto-detect which port to route traffic to, so we just let
# main.rs's 7878 fallback be the actual port in that case.
EXPOSE 7878
CMD ["astro-relay"]
