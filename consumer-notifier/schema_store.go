package main

import (
	"context"
	"database/sql"
)

type SchemaCache interface {
	Get(context.Context, string, int) (string, error)
	Remember(context.Context, string, int, string) (string, error)
}
type postgresSchemaCache struct{ db *sql.DB }

func (s postgresSchemaCache) Get(ctx context.Context, registry string, id int) (string, error) {
	var schema string
	err := s.db.QueryRowContext(ctx, `SELECT schema FROM schema_cache WHERE registry=$1 AND id=$2`, registry, id).Scan(&schema)
	return schema, err
}
func (s postgresSchemaCache) Remember(ctx context.Context, registry string, id int, schema string) (string, error) {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO schema_cache(registry,id,schema) VALUES($1,$2,$3) ON CONFLICT(registry,id) DO NOTHING`, registry, id, schema); err != nil {
		return "", err
	}
	return s.Get(ctx, registry, id)
}
