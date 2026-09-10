// Package httpclient da a los servicios un cliente HTTP uniforme.
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxDrainBytes limita cuánto cuerpo de respuesta se drena antes de cerrar,
// para no leer una respuesta de error arbitrariamente grande.
const maxDrainBytes = 64 << 10

// Client llama a otro servicio de la plataforma.
type Client struct {
	baseURL string
	http    *http.Client
}

// New crea un cliente contra la URL base de otro servicio.
func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 5 * time.Second}}
}

// GetJSON hace un GET y decodifica la respuesta JSON en out.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("construyendo petición: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("llamando a %s: %w", c.baseURL+path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		drain(resp.Body)
		return fmt.Errorf("%s devolvió %d", c.baseURL+path, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		drain(resp.Body)
		return fmt.Errorf("decodificando respuesta de %s: %w", c.baseURL+path, err)
	}
	return nil
}

// PostJSON hace un POST con el cuerpo codificado en JSON y decodifica la
// respuesta JSON en out. Añadido en la Task 13: gateway-service y
// order-service necesitan reenviar peticiones de creación/pago a otros
// servicios, y GetJSON solo cubre lecturas.
func (c *Client) PostJSON(ctx context.Context, path string, body, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("codificando petición a %s: %w", c.baseURL+path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("construyendo petición: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("llamando a %s: %w", c.baseURL+path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		drain(resp.Body)
		return fmt.Errorf("%s devolvió %d", c.baseURL+path, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		drain(resp.Body)
		return fmt.Errorf("decodificando respuesta de %s: %w", c.baseURL+path, err)
	}
	return nil
}

// drain lee (y descarta) lo que quede del cuerpo de la respuesta, hasta un
// límite razonable, para que el Transport pueda reutilizar la conexión TCP.
func drain(body io.Reader) {
	_, _ = io.CopyN(io.Discard, body, maxDrainBytes)
}
