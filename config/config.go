// Package config reads the service configuration from the environment.
package config

import "os"

// Config holds the settings shared by every Go service.
type Config struct {
	ServiceName string
	Port        string
	DatabaseURL string
	RedisURL    string
}

// Load reads the configuration from the environment, with development defaults.
//
// RedisURL reads the REDIS_URL variable, not REDIS_ADDR: REDIS_URL is the one
// emit-compose.ts really injects (see generator/src/emit-compose.ts, envFor)
// when a service declares a `redis:*` cache. With REDIS_ADDR the field was
// always empty because that variable is never set in any environment — a bug
// found in Task 13, while connecting seat-service to Redis for the seat
// lock.
func Load() Config {
	return Config{
		ServiceName: env("SERVICE_NAME", "unnamed-service"),
		Port:        env("PORT", "8080"),
		DatabaseURL: env("DATABASE_URL", ""),
		RedisURL:    env("REDIS_URL", ""),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
