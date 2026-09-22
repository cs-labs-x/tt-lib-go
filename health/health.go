// Package health exposes the health endpoint shared by every service.
package health

import (
	"encoding/json"
	"net/http"
)

// Handler returns the GET /health handler for the given service.
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
