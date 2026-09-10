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
		t.Errorf("ServiceName: esperaba seat-service, obtuve %q", cfg.ServiceName)
	}
	if cfg.Port != "9090" {
		t.Errorf("Port: esperaba 9090, obtuve %q", cfg.Port)
	}
	if cfg.DatabaseURL != "mysql://tt:tt@mysql:3306/seat" {
		t.Errorf("DatabaseURL inesperada: %q", cfg.DatabaseURL)
	}
	if cfg.RedisURL != "redis://redis:6379" {
		t.Errorf("RedisURL inesperada: %q", cfg.RedisURL)
	}
}

func TestLoadFallsBackWhenUnset(t *testing.T) {
	// t.Setenv registra el restore al terminar el test; el Unsetenv posterior
	// deja la variable AUSENTE de verdad (no vacía) durante la ejecución, que
	// es el caso que este test cubre.
	for _, key := range []string{"SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"} {
		t.Setenv(key, "placeholder")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("no se pudo limpiar %s: %v", key, err)
		}
	}

	cfg := Load()

	if cfg.ServiceName != "unnamed-service" {
		t.Errorf("ServiceName: esperaba unnamed-service, obtuve %q", cfg.ServiceName)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port: esperaba 8080, obtuve %q", cfg.Port)
	}
	if cfg.DatabaseURL != "" || cfg.RedisURL != "" {
		t.Errorf("esperaba URLs vacías, obtuve %q / %q", cfg.DatabaseURL, cfg.RedisURL)
	}
}

// Política común a las tres librerías: una variable de entorno VACÍA se
// trata como ausente, no como el valor "". Go ya lo hacía (el helper `env`
// compara con ""), Python también (`os.getenv(...) or <default>`), pero Node
// no — `Number(process.env.PORT ?? 8080)` con PORT="" daba 0, que para
// listen() significa "puerto efímero al azar". Este test fija la política
// aquí para que no se pierda al tocar `env`.
func TestLoadTreatsEmptyAsUnset(t *testing.T) {
	t.Setenv("SERVICE_NAME", "")
	t.Setenv("PORT", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")

	cfg := Load()

	if cfg.ServiceName != "unnamed-service" {
		t.Errorf("SERVICE_NAME vacía debe caer al valor por defecto, obtuve %q", cfg.ServiceName)
	}
	if cfg.Port != "8080" {
		t.Errorf("PORT vacía debe caer a 8080, obtuve %q", cfg.Port)
	}
	if cfg.DatabaseURL != "" || cfg.RedisURL != "" {
		t.Errorf("esperaba URLs vacías, obtuve %q / %q", cfg.DatabaseURL, cfg.RedisURL)
	}
}
