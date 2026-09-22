package config

import (
	"os"
	"testing"
)

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("SERVICE_NAME", "seat-service")
	t.Setenv("PORT", "9090")
	t.Setenv("DATABASE_URL", "mysql://tt:tt@mysql:3306/seat")
	t.Setenv("REDIS_URL", "redis://redis:6379")

	cfg := Load()

	if cfg.ServiceName != "seat-service" {
		t.Errorf("ServiceName: wanted seat-service, got %q", cfg.ServiceName)
	}
	if cfg.Port != "9090" {
		t.Errorf("Port: wanted 9090, got %q", cfg.Port)
	}
	if cfg.DatabaseURL != "mysql://tt:tt@mysql:3306/seat" {
		t.Errorf("unexpected DatabaseURL: %q", cfg.DatabaseURL)
	}
	if cfg.RedisURL != "redis://redis:6379" {
		t.Errorf("unexpected RedisURL: %q", cfg.RedisURL)
	}
}

func TestLoadFallsBackWhenUnset(t *testing.T) {
	// t.Setenv registers the restore for when the test ends; the Unsetenv that
	// follows leaves the variable really ABSENT (not empty) during the run,
	// which is the case this test covers.
	for _, key := range []string{"SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"} {
		t.Setenv(key, "placeholder")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("could not clear %s: %v", key, err)
		}
	}

	cfg := Load()

	if cfg.ServiceName != "unnamed-service" {
		t.Errorf("ServiceName: wanted unnamed-service, got %q", cfg.ServiceName)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port: wanted 8080, got %q", cfg.Port)
	}
	if cfg.DatabaseURL != "" || cfg.RedisURL != "" {
		t.Errorf("wanted empty URLs, got %q / %q", cfg.DatabaseURL, cfg.RedisURL)
	}
}

// Policy shared by the three libraries: an EMPTY environment variable is
// treated as absent, not as the value "". Go already did this (the `env`
// helper compares with ""), and Python too (`os.getenv(...) or <default>`),
// but Node did not — `Number(process.env.PORT ?? 8080)` with PORT="" gave 0,
// which for listen() means "a random ephemeral port". This test pins the
// policy here so it is not lost when `env` is touched.
func TestLoadTreatsEmptyAsUnset(t *testing.T) {
	t.Setenv("SERVICE_NAME", "")
	t.Setenv("PORT", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")

	cfg := Load()

	if cfg.ServiceName != "unnamed-service" {
		t.Errorf("an empty SERVICE_NAME must fall back to the default, got %q", cfg.ServiceName)
	}
	if cfg.Port != "8080" {
		t.Errorf("an empty PORT must fall back to 8080, got %q", cfg.Port)
	}
	if cfg.DatabaseURL != "" || cfg.RedisURL != "" {
		t.Errorf("wanted empty URLs, got %q / %q", cfg.DatabaseURL, cfg.RedisURL)
	}
}
