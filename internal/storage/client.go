package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a typed wrapper the gateway and workers use to talk to a storage
// node's HTTP API. One Client targets one node (identified by its base URL).
// It reuses a single http.Client so connections are pooled across many chunk
// transfers — creating a fresh client per request would exhaust sockets under
// load.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient targets a node at baseURL, e.g. "http://storage-1:8081".
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		// A generous timeout: chunk transfers stream up to 8 MiB. The gateway
		// wraps calls in its own context for per-request cancellation, so this
		// is a backstop against a wedged node, not the primary deadline.
		http: &http.Client{Timeout: 60 * time.Second},
	}
}

// BaseURL exposes the node's address (used for logging / rebalance copies).
func (c *Client) BaseURL() string { return c.baseURL }

// PutChunk streams body to the node under hash. The node re-verifies the hash;
// a 400 surfaces as ErrHashMismatch so the caller can retry on another node.
func (c *Client) PutChunk(ctx context.Context, hash string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.chunkURL(hash), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("put chunk %s to %s: %w", short(hash), c.baseURL, err)
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
		return nil
	case http.StatusBadRequest:
		return ErrHashMismatch
	default:
		return fmt.Errorf("put chunk %s to %s: unexpected status %d", short(hash), c.baseURL, resp.StatusCode)
	}
}

// GetChunk returns a streaming reader of the chunk bytes. Caller must Close it.
// ErrNotFound is returned for a 404 so callers can fail over to another replica.
func (c *Client) GetChunk(ctx context.Context, hash string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.chunkURL(hash), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get chunk %s from %s: %w", short(hash), c.baseURL, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		drain(resp)
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		drain(resp)
		return nil, fmt.Errorf("get chunk %s from %s: status %d", short(hash), c.baseURL, resp.StatusCode)
	}
	return resp.Body, nil // caller closes
}

// HasChunk reports whether the node holds the chunk (HEAD request).
func (c *Client) HasChunk(ctx context.Context, hash string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.chunkURL(hash), nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	drain(resp)
	return resp.StatusCode == http.StatusOK, nil
}

// DeleteChunk removes the chunk from the node (idempotent server-side).
func (c *Client) DeleteChunk(ctx context.Context, hash string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.chunkURL(hash), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	drain(resp)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete chunk %s on %s: status %d", short(hash), c.baseURL, resp.StatusCode)
	}
	return nil
}

// VerifyChunk asks the node to re-hash the stored bytes. Returns true if
// healthy, false if the node reports corruption (422), ErrNotFound if absent.
func (c *Client) VerifyChunk(ctx context.Context, hash string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.chunkURL(hash)+"/verify", nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusUnprocessableEntity:
		return false, nil
	case http.StatusNotFound:
		return false, ErrNotFound
	default:
		return false, fmt.Errorf("verify chunk %s on %s: status %d", short(hash), c.baseURL, resp.StatusCode)
	}
}

// ListChunks returns every hash the node stores. Used by workers.
func (c *Client) ListChunks(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/chunks", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list chunks on %s: status %d", c.baseURL, resp.StatusCode)
	}
	var out struct {
		Chunks []string `json:"chunks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Chunks, nil
}

// Health pings the node's liveness endpoint. Used by the gateway to keep the
// ring's membership in sync with which nodes are actually up.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	drain(resp)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node %s unhealthy: status %d", c.baseURL, resp.StatusCode)
	}
	return nil
}

func (c *Client) chunkURL(hash string) string {
	return c.baseURL + "/chunks/" + hash
}

// drain reads and closes the body so the underlying TCP connection can be
// reused from the pool. Skipping this leaks connections — a classic Go bug.
func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}
