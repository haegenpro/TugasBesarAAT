#!/bin/sh
set -e

nats-server -c /etc/nats/nats.conf &
NATS_PID=$!

cleanup() {
    kill -TERM "$NATS_PID" 2>/dev/null || true
    wait "$NATS_PID" 2>/dev/null || true
}

trap cleanup INT TERM

/usr/local/bin/broker-init

wait "$NATS_PID"
