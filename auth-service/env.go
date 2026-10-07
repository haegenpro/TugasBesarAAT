package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// requiredEnv validates configuration before opening storage or network connections.
// Error messages name settings without exposing their values or credentials.
func requiredEnv(keys ...string) (map[string]string, error) {
	values := make(map[string]string, len(keys))
	var missing []string
	for _, key := range keys {
		values[key] = os.Getenv(key)
		if strings.TrimSpace(values[key]) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("required environment variables missing or empty: %s", strings.Join(missing, ", "))
	}
	return values, nil
}

func envInt(values map[string]string, key string, min, max int) (int, error) {
	n, err := strconv.Atoi(values[key])
	if err != nil || n < min || (max > 0 && n > max) {
		if max > 0 {
			return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max)
		}
		return 0, fmt.Errorf("%s must be an integer >= %d", key, min)
	}
	return n, nil
}

func envURL(values map[string]string, key string, schemes ...string) error {
	u, err := url.Parse(values[key])
	if err == nil && u.Hostname() != "" {
		for _, scheme := range schemes {
			if u.Scheme == scheme {
				return nil
			}
		}
	}
	return fmt.Errorf("%s must be an absolute URL with scheme %s and a host", key, strings.Join(schemes, " or "))
}
