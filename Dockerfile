# Build from the NEM Lens root, which holds monitor/ and aemo/ side by side:
#   docker build -f monitor/Dockerfile -t nem-monitor .
# The mounted config must set [server] bind = "0.0.0.0:8080" (the default is localhost).
FROM rust:1-bookworm AS build
WORKDIR /src
COPY aemo aemo
COPY monitor/Cargo.toml monitor/Cargo.lock monitor/
COPY monitor/src monitor/src
COPY monitor/static monitor/static
WORKDIR /src/monitor
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home /data nem && mkdir /data && chown nem /data
COPY --from=build /src/monitor/target/release/nem-monitor /usr/local/bin/nem-monitor
USER nem
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["tini", "--", "nem-monitor"]
CMD ["-c", "/etc/nem-monitor/config.toml", "serve"]
