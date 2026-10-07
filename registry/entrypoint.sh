#!/bin/sh
set -eu

# Validate configuration once. Missing settings must never become retry loops.
require_setting() {
    case "$2" in
        *[![:space:]]*) ;;
        *) printf 'invalid configuration: %s is required\n' "$1" >&2; exit 1 ;;
    esac
}
require_setting APICURIO_STORAGE_KIND "${APICURIO_STORAGE_KIND-}"
require_setting APICURIO_STORAGE_SQL_KIND "${APICURIO_STORAGE_SQL_KIND-}"
require_setting APICURIO_DATASOURCE_URL "${APICURIO_DATASOURCE_URL-}"
require_setting APICURIO_DATASOURCE_USERNAME "${APICURIO_DATASOURCE_USERNAME-}"
require_setting APICURIO_DATASOURCE_PASSWORD "${APICURIO_DATASOURCE_PASSWORD-}"
require_setting REGISTRY_RETRY_DELAY_SEC "${REGISTRY_RETRY_DELAY_SEC-}"
case "$REGISTRY_RETRY_DELAY_SEC" in
    *[!0-9]*) printf 'invalid configuration: REGISTRY_RETRY_DELAY_SEC must be positive integer seconds\n' >&2; exit 1 ;;
esac
if ! [ "$REGISTRY_RETRY_DELAY_SEC" -gt 0 ] 2>/dev/null; then
    printf 'invalid configuration: REGISTRY_RETRY_DELAY_SEC must be positive integer seconds\n' >&2
    exit 1
fi
if [ "$APICURIO_STORAGE_KIND" != sql ] || [ "$APICURIO_STORAGE_SQL_KIND" != postgresql ]; then
    printf 'invalid configuration: this Registry requires sql storage with postgresql\n' >&2
    exit 1
fi
case "$APICURIO_DATASOURCE_URL" in
    jdbc:postgresql://*) ;;
    *) printf 'invalid configuration: APICURIO_DATASOURCE_URL must be a PostgreSQL JDBC URL\n' >&2; exit 1 ;;
esac
if [ "$#" -eq 0 ]; then
    printf 'invalid configuration: Registry launch command is required\n' >&2
    exit 1
fi

child_pid=
stop() {
    trap - TERM INT
    if [ -n "$child_pid" ]; then
        kill -TERM "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
    fi
    exit 0
}
trap stop TERM INT

# Keep this container alive through an absent database or exhausted upstream
# startup retries. PostgreSQL remains the only storage backend. Apicurio's
# readiness endpoint stays unavailable until its initialization succeeds.
while :; do
    printf '{"service":"schema-registry","event":"starting_registry"}\n'
    "$@" &
    child_pid=$!
    status=0
    wait "$child_pid" || status=$?
    child_pid=
    printf '{"service":"schema-registry","event":"registry_exited","exit_code":%s,"retry_delay_sec":"%s"}\n' "$status" "$REGISTRY_RETRY_DELAY_SEC"
    sleep "$REGISTRY_RETRY_DELAY_SEC" &
    child_pid=$!
    wait "$child_pid" || true
    child_pid=
done
