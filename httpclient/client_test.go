package httpclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetJSONDecodesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/seat/available" {
			t.Errorf("ruta inesperada: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"count":42}`))
	}))
	defer srv.Close()

	var out struct{ Count int `json:"count"` }
	if err := New(srv.URL).GetJSON(context.Background(), "/api/v1/seat/available", &out); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if out.Count != 42 {
		t.Fatalf("esperaba 42, obtuve %d", out.Count)
	}
}

func TestGetJSONFailsOnServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	var out struct{}
	if err := New(srv.URL).GetJSON(context.Background(), "/x", &out); err == nil {
		t.Fatal("esperaba error en respuesta 500")
	}
}

func TestPostJSONSendsBodyAndDecodesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("método inesperado: %s", r.Method)
		}
		if r.URL.Path != "/api/v1/order" {
			t.Errorf("ruta inesperada: %s", r.URL.Path)
		}
		var in struct {
			TravelID string `json:"travelId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatalf("no se pudo decodificar el cuerpo enviado: %v", err)
		}
		if in.TravelID != "t-1" {
			t.Fatalf("esperaba travelId=t-1, obtuve %q", in.TravelID)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"o-1"}`))
	}))
	defer srv.Close()

	var out struct {
		ID string `json:"id"`
	}
	body := map[string]string{"travelId": "t-1"}
	if err := New(srv.URL).PostJSON(context.Background(), "/api/v1/order", body, &out); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if out.ID != "o-1" {
		t.Fatalf("esperaba id=o-1, obtuve %q", out.ID)
	}
}

func TestPostJSONFailsOnServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	var out struct{}
	if err := New(srv.URL).PostJSON(context.Background(), "/x", map[string]string{}, &out); err == nil {
		t.Fatal("esperaba error en respuesta 409")
	}
}

func TestGetJSONFailsOnNonJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("no soy json"))
	}))
	defer srv.Close()

	var out struct{}
	err := New(srv.URL).GetJSON(context.Background(), "/x", &out)
	if err == nil {
		t.Fatal("esperaba error al decodificar un cuerpo no-JSON")
	}
	wantURL := srv.URL + "/x"
	if !strings.Contains(err.Error(), wantURL) {
		t.Fatalf("esperaba que el error mencionara %q, obtuve: %v", wantURL, err)
	}
}
