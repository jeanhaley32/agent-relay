package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// roundTripFunc lets a test stand in for the http transport without a network.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestClient(rt roundTripFunc) *hypatiaClient {
	return &hypatiaClient{
		url:    "https://hypatia.test/api/chat/completions",
		apiKey: "test-key",
		model:  "granite",
		http:   &http.Client{Transport: rt},
	}
}

func jsonResp(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

// TestGenerateHappyPath: a normal completion returns the assistant text, and
// the request must carry the bearer token, the model, and the 8k output cap.
func TestGenerateHappyPath(t *testing.T) {
	var gotBody hypatiaReq
	c := newTestClient(func(r *http.Request) (*http.Response, error) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("missing/wrong auth header: %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		return jsonResp(200, `{"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"scaffold"}}]}`), nil
	})

	out, err := c.generate(context.Background(), "make a scaffold", "be terse")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "scaffold" {
		t.Fatalf("got %q", out)
	}
	if gotBody.Model != "granite" || gotBody.MaxTokens != hypatiaMaxTokens {
		t.Fatalf("bad request: model=%q max=%d", gotBody.Model, gotBody.MaxTokens)
	}
	if len(gotBody.Messages) != 2 || gotBody.Messages[0].Role != "system" || gotBody.Messages[1].Role != "user" {
		t.Fatalf("bad messages: %+v", gotBody.Messages)
	}
}

// TestGenerateOmitsEmptySystem: no system message when none is supplied.
func TestGenerateOmitsEmptySystem(t *testing.T) {
	var gotBody hypatiaReq
	c := newTestClient(func(r *http.Request) (*http.Response, error) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		return jsonResp(200, `{"choices":[{"finish_reason":"stop","message":{"content":"x"}}]}`), nil
	})
	if _, err := c.generate(context.Background(), "hi", "  "); err != nil {
		t.Fatal(err)
	}
	if len(gotBody.Messages) != 1 || gotBody.Messages[0].Role != "user" {
		t.Fatalf("expected only a user message, got %+v", gotBody.Messages)
	}
}

// TestGenerateTruncationNote: a length-capped finish is flagged so the caller
// knows the artifact is incomplete rather than silently trusting it.
func TestGenerateTruncationNote(t *testing.T) {
	c := newTestClient(func(*http.Request) (*http.Response, error) {
		return jsonResp(200, `{"choices":[{"finish_reason":"length","message":{"content":"partial"}}]}`), nil
	})
	out, err := c.generate(context.Background(), "big", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "truncated") {
		t.Fatalf("expected truncation note, got %q", out)
	}
}

// TestGenerateAuthError: a 401 maps to a clear, actionable message.
func TestGenerateAuthError(t *testing.T) {
	c := newTestClient(func(*http.Request) (*http.Response, error) {
		return jsonResp(401, `{"error":"unauthorized"}`), nil
	})
	_, err := c.generate(context.Background(), "x", "")
	if err == nil || !strings.Contains(err.Error(), "HYPATIA_API_KEY") {
		t.Fatalf("expected auth error naming the env var, got %v", err)
	}
}

// TestGenerateMissingKey: no network call happens without a key.
func TestGenerateMissingKey(t *testing.T) {
	c := newTestClient(func(*http.Request) (*http.Response, error) {
		t.Fatal("should not hit the network without a key")
		return nil, nil
	})
	c.apiKey = ""
	if _, err := c.generate(context.Background(), "x", ""); err == nil {
		t.Fatal("expected an error when the key is unset")
	}
}
