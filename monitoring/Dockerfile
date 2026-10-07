FROM golang:1.27.1-alpine3.24 AS launcher
WORKDIR /src
COPY launcher/go.mod launcher/main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /monitoring-launch .

FROM grafana/grafana:13.2.3 AS grafana
COPY --from=launcher /monitoring-launch /usr/local/bin/monitoring-launch
ENTRYPOINT ["/usr/local/bin/monitoring-launch", "grafana", "/run.sh"]

FROM prom/prometheus:v3.15.0 AS prometheus
COPY --from=launcher /monitoring-launch /usr/local/bin/monitoring-launch
ENTRYPOINT ["/usr/local/bin/monitoring-launch", "prometheus", "/bin/prometheus"]
CMD ["--config.file=/tmp/monitoring/prometheus.yml", "--storage.tsdb.path=/prometheus", "--web.enable-remote-write-receiver"]

FROM grafana/loki:3.7.8 AS loki
COPY --from=launcher /monitoring-launch /usr/local/bin/monitoring-launch
ENTRYPOINT ["/usr/local/bin/monitoring-launch", "loki", "/usr/bin/loki"]
CMD ["-config.file=/tmp/monitoring/loki.yml"]

FROM grafana/alloy:v1.20.1 AS alloy
COPY --from=launcher /monitoring-launch /usr/local/bin/monitoring-launch
ENTRYPOINT ["/usr/local/bin/monitoring-launch", "alloy", "/bin/alloy"]
CMD ["run", "/tmp/monitoring/config.alloy", "--storage.path=/var/lib/alloy/data", "--server.http.listen-addr=0.0.0.0:12345"]

FROM natsio/prometheus-nats-exporter:0.20.2 AS nats-exporter
COPY --from=launcher /monitoring-launch /usr/local/bin/monitoring-launch
ENTRYPOINT ["/usr/local/bin/monitoring-launch", "nats-exporter", "/usr/local/bin/prometheus-nats-exporter"]
CMD ["-addr=0.0.0.0", "-port=7777", "-path=/metrics", "-varz", "-jsz=all"]
