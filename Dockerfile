FROM rust:bookworm AS builder

RUN apt-get update && \
	apt install -y musl-tools musl-dev libssl-dev clang mold

# RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin
RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C /usr/local/bin
# RUN cargo install cargo-nextest --locked

WORKDIR /ark-invest-api-rust-data

COPY . .

RUN rustup target add x86_64-unknown-linux-musl && rustup update && cargo update

RUN --mount=type=cache,target=/usr/local/cargo,from=rust,source=/usr/local/cargo \
	--mount=type=cache,target=./target \
	cargo build --target x86_64-unknown-linux-musl --release && \
	cp ./target/target/x86_64-unknown-linux-musl/release/ark-invest-api-rust-data .

FROM builder AS test

RUN --mount=type=cache,target=/usr/local/cargo,from=rust,source=/usr/local/cargo \
	--mount=type=cache,target=./target \
	cargo nextest run --release --target x86_64-unknown-linux-musl \
	-E "all() - test(get_api) - kind(bin)"

FROM alpine:latest AS main

WORKDIR /ark-invest-api-rust-data

COPY --from=builder ark-invest-api-rust-data/ark-invest-api-rust-data .

CMD ["./ark-invest-api-rust-data"]
