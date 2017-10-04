# taken from https://github.com/rust-fuzz/afl.rs/blob/master/Dockerfile
# but modified to not lock the date of the compiler release

FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
  curl \
  file \
  g++ \
  g++-multilib \
  g++-4.9 \
  g++-4.9-multilib \
  libstdc++-4.8-dev \
  llvm-3.8 \
  make \
  python \
  git \
  libssl-dev \
  openssl

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CXX /usr/bin/g++-4.9

RUN curl -sSf https://static.rust-lang.org/rustup.sh | sh -s -- --channel=nightly --date=2017-04-01 --disable-sudo

RUN curl http://lcamtuf.coredump.cx/afl/releases/afl-2.34b.tgz > afl-2.34b.tgz && \
  tar xf afl-2.34b.tgz && \
  cd afl-2.34b && \
  make && \
  make install && \
  cd .. && \
  rm -rf afl-2.34.tgz

WORKDIR /source
