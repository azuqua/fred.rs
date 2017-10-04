# Fuzz Testing

First, get around [this](https://github.com/rust-lang/cargo/issues/1423) by `cp Cargo.toml.bk Cargo.toml`. Note that `Cargo.toml` in this directory is already in the .gitignore.

Read this but don't do it: https://rust-fuzz.github.io/afl.rs/introduction.html 

## Run

The steps in the above link stopped working earlier this year and now requires that you run a certain build of the Rust compiler that is ABI compatible with afl.rs and still has an up to date STL.

### First Install

1. Build and install AFL if it's not already installed.

```
wget http://lcamtuf.coredump.cx/afl/releases/afl-latest.tgz
tar xvfz afl-latest.tgz
cd afl-latest
make
sudo make install
```

2. [Install docker](https://docs.docker.com/engine/installation/) if it's not already installed.

3. Build and install the docker image. 

```
cd path/to/redis/tests/fuzz/docker
sudo docker build -t test-afl-redis .
```

4. Make sure the `afl.rs` dependency lines in the [parent's Cargo.toml](../../Cargo.toml) are uncommented.

5. Build the fuzzer.

```
cd path/to/redis
cargo clean
cd tests/fuzz
sudo docker run -v $(pwd):/source -v $(pwd)/../..:/source/redis.rs -it test-afl-redis bash
```

Then, inside the container:

```
cargo build
exit
```

6. Run the fuzzer.

```
afl-fuzz -i in -o out target/debug/redis_fuzzer
```

This might create a `redis.rs` folder in your test/fuzz folder, but don't worry about it. It's a side effect of mounting a docker volume there and it's already in the .gitignore.

When you're done fuzzing don't forget to comment out the `afl.rs` dependency lines in the parent's Cargo.toml. Also, don't forget to remove the copy of the `Cargo.toml` in this directory, but keep the backup.
