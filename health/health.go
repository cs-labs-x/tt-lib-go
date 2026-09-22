// Package health expone el endpoint de salud común a todos los servicios.
package health

import (
	"encoding/json"
	"net/http"
)

// Handler devuelve el manejador de GET /health para el servicio indicado.
func Handler(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "ok",
			"service": serviceName,
		})
	}
}
