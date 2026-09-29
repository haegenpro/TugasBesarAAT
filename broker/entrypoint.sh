#!/bin/sh
set -eu

rm -f /tmp/broker-ready
nats-server --config /etc/nats/nats.conf &
nats_pid=$!

cleanup() {
    kill -TERM "$nats_pid" 2>/dev/null || true
    wait "$nats_pid" 2>/dev/null || true
}

trap cleanup EXIT
trap 'exit 0' INT TERM

/usr/local/bin/broker-init
touch /tmp/broker-ready
wait "$nats_pid"
