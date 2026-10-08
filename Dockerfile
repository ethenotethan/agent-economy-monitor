# syntax=docker/dockerfile:1.7
FROM rust:1.90-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release --bin agent-economy-monitor

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /workspace/target/release/agent-economy-monitor /agent-economy-monitor
ENV RUST_LOG=info
EXPOSE 8080
ENTRYPOINT ["/agent-economy-monitor"]
CMD ["serve"]
