# Stage 1: Build the release binary natively for the target platform
FROM rust:slim AS builder
WORKDIR /app

# Copy dependency manifests
COPY Cargo.toml Cargo.lock ./

# Create a dummy source to pre-compile and cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# Copy the actual source code
COPY src ./src

# Force rebuild with real source files and output optimized binary
RUN touch src/main.rs && cargo build --release

# Stage 2: Minimal lightweight runtime container
FROM ubuntu:24.04
WORKDIR /app

# Copy the compiled standalone binary from the builder stage
COPY --from=builder /app/target/release/pipistrelle /app/pipistrelle

# Expose standard and custom ports:
# 1883: TCP, 8883: TLS, 8083: WebSockets, 9090: Prometheus Metrics
EXPOSE 1883 8883 8083 9090

# Default command to start the broker
ENTRYPOINT ["/app/pipistrelle"]
