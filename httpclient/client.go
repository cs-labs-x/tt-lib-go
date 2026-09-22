// Package httpclient gives the services one uniform HTTP client.
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

// maxDrainBytes caps how much of a response body is drained before closing,
// so an arbitrarily large error response is never read whole.
const maxDrainBytes = 64 << 10

// Client calls another service of the platform.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a client against the base URL of another service.
func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 5 * time.Second}}
}

// GetJSON does a GET and decodes the JSON response into out.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling %s: %w", c.baseURL+path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		drain(resp.Body)
		return fmt.Errorf("%s returned %d", c.baseURL+path, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		drain(resp.Body)
		return fmt.Errorf("decoding response from %s: %w", c.baseURL+path, err)
	}
	return nil
}

// PostJSON does a POST with the body encoded as JSON and decodes the JSON
// response into out. Added in Task 13: gateway-service and order-service
// need to forward create/pay requests to other services, and GetJSON only
// covers reads.
func (c *Client) PostJSON(ctx context.Context, path string, body, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encoding request to %s: %w", c.baseURL+path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("calling %s: %w", c.baseURL+path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		drain(resp.Body)
		return fmt.Errorf("%s returned %d", c.baseURL+path, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		drain(resp.Body)
		return fmt.Errorf("decoding response from %s: %w", c.baseURL+path, err)
	}
	return nil
}

// drain reads (and discards) whatever is left of the response body, up to a
// reasonable limit, so the Transport can reuse the TCP connection.
func drain(body io.Reader) {
	_, _ = io.CopyN(io.Discard, body, maxDrainBytes)
}
