# Targets:
#   test  runs the unit and in-process tests
#   cli   a small image with the valkvedge-cli binary
FROM rust:1-bookworm AS base
WORKDIR /src
COPY . .

FROM base AS test
RUN cargo test --workspace --locked

FROM base AS build
RUN cargo build --release --locked -p valkvedge-cli

FROM debian:bookworm-slim AS cli
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home valkvedge
COPY --from=build /src/target/release/valkvedge-cli /usr/local/bin/valkvedge-cli
USER valkvedge
ENTRYPOINT ["valkvedge-cli"]
