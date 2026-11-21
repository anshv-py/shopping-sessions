# Shopping Sessions: a static Rust binary on an empty image.
FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64-v3"
RUN cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions \
 && strip /sessions

FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
ENTRYPOINT ["/sessions"]
