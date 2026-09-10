package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlerReturnsOk(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler("seat-service")(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, obtuve %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("respuesta no es JSON: %v", err)
	}
	if body["status"] != "ok" || body["service"] != "seat-service" {
		t.Fatalf("cuerpo inesperado: %v", body)
	}
}
