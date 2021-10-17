#!/bin/sh
mkdir bin
cd mock && go build -o mock-generator main.go && mv mock-generator ../bin/
cd ../ristretto-go && go build -o ristretto-go main.go && mv ristretto-go ../bin/
cd ../ristretto-rs && cargo build --features sync --release && mv target/release/ristretto-rs ../bin/ristretto-sync-rs && cargo build --features async --release && mv target/release/ristretto-rs ../bin/ristretto-async-rs
cd ../moka-rs && cargo build --features async --release && mv target/release/moka-rs ../bin/moka-async-rs && cargo build --features sync --release && mv target/release/moka-rs ../bin/moka-sync-rs
cd ../bin && ./mock-generator && ./ristretto-go && ./ristretto-sync-rs && ./ristretto-async-rs && ./moka-sync-rs && ./moka-async-rs
