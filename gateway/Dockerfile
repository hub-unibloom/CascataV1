# =============================================================================
# cascata/gateway — Dockerfile
# =============================================================================
# Multi-stage: builder (Rust compile) → runtime (Debian slim).
# Zero ferramentas de build no runtime.
# Ref: TASK PR-6, SAD §B.
# =============================================================================

# Stage 1: Builder
FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache de dependências — Cargo.toml/Cargo.lock mudam menos que o código
COPY Cargo.toml Cargo.lock* rust-toolchain.toml* ./

# Criar src/main.rs dummy para compilar dependências
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src

# Copiar código real e compilar
COPY . .
RUN cargo build --release

# Stage 2: Runtime — imagem mínima com apenas libs necessárias
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Criar user não-root
RUN useradd -r -s /usr/sbin/nologin -d /app cascata
USER cascata
WORKDIR /app

# Copiar binário do builder
COPY --from=builder /build/target/release/cascata-gateway .

# Copiar config de dev (sobrescrita por mount em produção)
COPY --from=builder /build/gateway.dev.toml /etc/cascata/gateway.toml

EXPOSE 8080 8443

ENV CASCATA_GATEWAY_CONFIG=/etc/cascata/gateway.toml
ENV RUST_LOG=info

ENTRYPOINT ["./cascata-gateway"]
