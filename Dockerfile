FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY static static
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home /data nem && mkdir /data && chown nem /data
COPY --from=build /src/target/release/nem-monitor /usr/local/bin/nem-monitor
USER nem
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["tini", "--", "nem-monitor"]
CMD ["-c", "/etc/nem-monitor/config.toml", "serve"]
