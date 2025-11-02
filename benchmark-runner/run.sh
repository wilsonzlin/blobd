#!/usr/bin/env bash

set -Eeuo pipefail

cargo run --profile release-max -- --objects  567890 --object-size    12345 --concurrency 1024
cargo run --profile release-max -- --objects  400890 --object-size    78901 --concurrency 1024
cargo run --profile release-max -- --objects  300890 --object-size   345678 --concurrency 1024
cargo run --profile release-max -- --objects  123456 --object-size  1512345 --concurrency 1024
cargo run --profile release-max -- --objects   45678 --object-size  4012345 --concurrency 1024
cargo run --profile release-max -- --objects   17890 --object-size 10123123 --concurrency 1024
cargo run --profile release-max -- --objects    7004 --object-size 31987654 --concurrency 1024
