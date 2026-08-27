#!/usr/bin/env bash
# Build and run both implementations, printing results back-to-back.
set -e
cd "$(dirname "$0")"

echo "== building rust =="
(cd rust && cargo build --release 2>&1 | tail -1)
echo "== building java =="
(cd java && mvn -q package)
echo "== building go =="
(cd go && go build -o clob-bench .)

echo
echo "==================== RUST ===================="
./rust/target/release/clob-bench all
echo
echo "==================== JAVA ===================="
java -jar java/target/clob-bench-0.1.0.jar all
echo
echo "==================== GO ======================"
./go/clob-bench all
echo
echo "Checksums must match across all three runs; if they do, both engines"
echo "processed a bit-identical workload identically."
