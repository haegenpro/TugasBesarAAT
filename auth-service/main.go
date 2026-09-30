package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig()
	if err != nil {
		log.Error("invalid configuration", "service", "auth-service", "error", err)
		os.Exit(1)
	}
	store, err := openStore(cfg.DBPath, cfg.AccessTTL, cfg.RefreshTTL)
	if err != nil {
		log.Error("cannot open auth storage", "service", "auth-service", "error", err)
		os.Exit(1)
	}
	defer store.db.Close()
	server := &http.Server{Addr: ":" + cfg.Port, Handler: (&authServer{config: cfg, store: store, log: log}).router(), ReadHeaderTimeout: 5 * time.Second}
	log.Info("auth service listening", "service", "auth-service", "port", cfg.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Error("auth service stopped", "service", "auth-service", "error", err)
		os.Exit(1)
	}
}
