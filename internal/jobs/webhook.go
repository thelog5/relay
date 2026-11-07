package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"syscall"
	"time"
)

// WebhookPayload is {"url": "https://example.com/hook", "body": {...}}.
// The body is POSTed as JSON. timeout_ms defaults to 5000.
type WebhookPayload struct {
	URL       string          `json:"url"`
	Body      json.RawMessage `json:"body,omitempty"`
	TimeoutMS int             `json:"timeout_ms,omitempty"`
}

// WebhookHandler POSTs the payload body to a URL (R2).
//
// Because delivery is at-least-once, every request carries an
// Idempotency-Key header equal to the job ID. The receiver should dedupe on
// it; retries of the same job always send the same key.
type WebhookHandler struct {
	Client *http.Client
}

// NewWebhookHandler returns a handler whose client refuses to connect to
// link-local addresses (cloud metadata endpoints such as 169.254.169.254).
func NewWebhookHandler() *WebhookHandler {
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip != nil && (ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()) {
				return fmt.Errorf("webhook to link-local address %s is blocked", ip)
			}
			return nil
		},
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = dialer.DialContext
	return &WebhookHandler{Client: &http.Client{Transport: transport}}
}

func (WebhookHandler) parse(payload json.RawMessage) (WebhookPayload, error) {
	var p WebhookPayload
	if err := decode(payload, &p); err != nil {
		return p, err
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return p, errors.New("url must be an absolute http(s) URL")
	}
	if p.TimeoutMS < 0 || p.TimeoutMS > 30000 {
		return p, errors.New("timeout_ms must be between 0 and 30000")
	}
	if p.TimeoutMS == 0 {
		p.TimeoutMS = 5000
	}
	if len(p.Body) == 0 {
		p.Body = json.RawMessage("{}")
	}
	return p, nil
}

func (h WebhookHandler) Validate(payload json.RawMessage) error {
	_, err := h.parse(payload)
	return err
}

func (h WebhookHandler) Handle(ctx context.Context, job *Job) error {
	p, err := h.parse(job.Payload)
	if err != nil {
		return Permanent(err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(p.TimeoutMS)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(p.Body))
	if err != nil {
		return Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "relay-webhook/1")
	req.Header.Set("Idempotency-Key", job.ID)
	req.Header.Set("X-Relay-Job-Id", job.ID)
	req.Header.Set("X-Relay-Attempt", strconv.Itoa(job.Attempts))

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	default:
		// Other 4xx: the receiver rejected the request; retrying will not help.
		return Permanent(fmt.Errorf("webhook returned %d", resp.StatusCode))
	}
}
