#!/bin/sh

set -ue

rm -rf bin
mkdir -p bin

cd mock
go build -o mock-generator main.go
cp mock-generator ../bin/
cd ..

cd ristretto-go
go build -o ristretto-go main.go
cp ristretto-go ../bin/
cd ..

cd ristretto-rs
cargo build --features sync --release
cp target/release/ristretto-rs ../bin/ristretto-sync-rs
cargo build --features async --release
cp target/release/ristretto-rs ../bin/ristretto-async-rs
cd ..

cd moka-rs
cargo build --features async --release
cp target/release/moka-rs ../bin/moka-async-rs
cargo build --features sync --release
cp target/release/moka-rs ../bin/moka-sync-rs
cd ..

cd bin
./mock-generator
./ristretto-go
./ristretto-sync-rs
./ristretto-async-rs
./moka-sync-rs
./moka-async-rs
cd ..
