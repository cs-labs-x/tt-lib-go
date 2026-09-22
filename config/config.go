// Package config lee la configuración del servicio desde el entorno.
package config

import "os"

// Config son los ajustes comunes a todos los servicios Go.
type Config struct {
	ServiceName string
	Port        string
	DatabaseURL string
	RedisURL    string
}

// Load lee la configuración del entorno, con valores por defecto de desarrollo.
//
// RedisURL lee la variable REDIS_URL, no REDIS_ADDR: es la que
// emit-compose.ts inyecta de verdad (ver generator/src/emit-compose.ts,
// envFor) cuando un servicio declara una cache `redis:*`. Con REDIS_ADDR el
// campo quedaba siempre vacío porque esa variable nunca se establece en
// ningún entorno — bug detectado en la Task 13, al conectar seat-service a
// Redis para el lock de asientos.
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
