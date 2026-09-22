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
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"count":42}`))
	}))
	defer srv.Close()

	var out struct {
		Count int `json:"count"`
	}
	if err := New(srv.URL).GetJSON(context.Background(), "/api/v1/seat/available", &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Count != 42 {
		t.Fatalf("wanted 42, got %d", out.Count)
	}
}

func TestGetJSONFailsOnServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	var out struct{}
	if err := New(srv.URL).GetJSON(context.Background(), "/x", &out); err == nil {
		t.Fatal("wanted an error on a 500 response")
	}
}

func TestPostJSONSendsBodyAndDecodesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != "/api/v1/order" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var in struct {
			TravelID string `json:"travelId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatalf("could not decode the body that was sent: %v", err)
		}
		if in.TravelID != "t-1" {
			t.Fatalf("wanted travelId=t-1, got %q", in.TravelID)
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
		t.Fatalf("unexpected error: %v", err)
	}
	if out.ID != "o-1" {
		t.Fatalf("wanted id=o-1, got %q", out.ID)
	}
}

func TestPostJSONFailsOnServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	var out struct{}
	if err := New(srv.URL).PostJSON(context.Background(), "/x", map[string]string{}, &out); err == nil {
		t.Fatal("wanted an error on a 409 response")
	}
}

func TestGetJSONFailsOnNonJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("i am not json"))
	}))
	defer srv.Close()

	var out struct{}
	err := New(srv.URL).GetJSON(context.Background(), "/x", &out)
	if err == nil {
		t.Fatal("wanted an error when decoding a non-JSON body")
	}
	wantURL := srv.URL + "/x"
	if !strings.Contains(err.Error(), wantURL) {
		t.Fatalf("wanted the error to mention %q, got: %v", wantURL, err)
	}
}
