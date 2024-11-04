# syntax=docker/dockerfile:1
FROM archlinux:base-devel
RUN pacman -Syu --noconfirm && pacman -S --noconfirm rustup protobuf
RUN rustup default nightly
COPY . /app
WORKDIR /app
RUN cargo build --release

FROM archlinux:base
COPY --from=0 /app/target/release/* /usr/local/bin/
