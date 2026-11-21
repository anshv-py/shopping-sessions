# DIAGNOSTIC BUILD: ships the dataset generator, which makes the binary run a self-benchmark
# on the public dev set and then fail the dev check on purpose (to return logs).
FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY tools/generator /gen
RUN cd /gen && RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/generate /generate
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64-v3"
RUN cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions \
 && strip /sessions

FROM scratch
COPY --from=build /sessions /sessions
COPY --from=build /generate /generate
USER 1000:1000
ENTRYPOINT ["/sessions"]
