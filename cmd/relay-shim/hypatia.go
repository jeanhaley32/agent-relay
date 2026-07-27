package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/channel"
	"github.com/jeanhaley32/agent-relay/internal/mcp"
)

// Hypatia is a self-hosted Open WebUI instance serving IBM Granite 4.0-H-Small
// over an OpenAI-compatible chat/completions API. Its output tokens are free
// (the box is already paid for), so the point of this tool is to move long,
// mechanical generation OFF the Claude session — cutting Claude-side dollar
// cost at equal accuracy — NOT to reduce total tokens. See DESIGN.md.

const (
	hypatiaDefaultURL   = "https://hypatia.byatt.io/api/chat/completions"
	hypatiaDefaultModel = "/srv/models/granite-4.0-h-small/granite-4.0-h-small-UD-Q4_K_XL.gguf"
	hypatiaMaxTokens    = 8192            // hard output cap of the served model
	hypatiaTimeout      = 4 * time.Minute // generating a full 8k artifact is slow
)

// hypatiaClient calls the Hypatia chat/completions endpoint. Fields are wired
// from the environment by registerHypatiaTool; the http.Client is injected so
// the handler is testable against a mock transport.
type hypatiaClient struct {
	url    string
	apiKey string
	model  string
	http   *http.Client
}

type hypatiaReq struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	Messages  []hypatiaMsg `json:"messages"`
}

type hypatiaMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type hypatiaResp struct {
	Choices []struct {
		FinishReason string     `json:"finish_reason"`
		Message      hypatiaMsg `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// generate performs one blocking completion and returns the assistant text.
func (c *hypatiaClient) generate(ctx context.Context, prompt, system string) (string, error) {
	if c.apiKey == "" {
		return "", errors.New("HYPATIA_API_KEY is not set — populate it in .env (see cmd/relay-shim/DESIGN.md)")
	}
	msgs := make([]hypatiaMsg, 0, 2)
	if strings.TrimSpace(system) != "" {
		msgs = append(msgs, hypatiaMsg{Role: "system", Content: system})
	}
	msgs = append(msgs, hypatiaMsg{Role: "user", Content: prompt})

	body, err := json.Marshal(hypatiaReq{Model: c.model, MaxTokens: hypatiaMaxTokens, Messages: msgs})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("hypatia request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return "", fmt.Errorf("hypatia auth rejected (HTTP %d) — check HYPATIA_API_KEY in .env", resp.StatusCode)
		case http.StatusRequestEntityTooLarge, http.StatusBadRequest:
			return "", fmt.Errorf("hypatia rejected the request (HTTP %d) — likely over the 64k context limit; shorten the prompt: %s", resp.StatusCode, trunc(raw, 300))
		default:
			return "", fmt.Errorf("hypatia HTTP %d: %s", resp.StatusCode, trunc(raw, 300))
		}
	}

	var hr hypatiaResp
	if err := json.Unmarshal(raw, &hr); err != nil {
		return "", fmt.Errorf("decoding hypatia response: %w", err)
	}
	if hr.Error != nil {
		return "", fmt.Errorf("hypatia error: %s", hr.Error.Message)
	}
	if len(hr.Choices) == 0 {
		return "", errors.New("hypatia returned no choices")
	}
	out := hr.Choices[0].Message.Content
	if hr.Choices[0].FinishReason == "length" {
		out += "\n\n[hypatia: output truncated at the 8k-token limit — the artifact is incomplete]"
	}
	return out, nil
}

func trunc(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// hypatiaDescription is the interface, not just docs: it steers callers toward
// good-fit delegations and away from bad ones. Keep the fit criteria here, not
// only in DESIGN.md — the model reads this string to decide whether to call.
const hypatiaDescription = "Delegate a fully-specified, mechanical generation task to Hypatia " +
	"(a self-hosted Granite 4.0-H-Small model). Its output is FREE, so use it to move long, rote " +
	"output OFF this session and cut cost — the win is Claude-side dollars saved, not total tokens.\n\n" +
	"USE IT when a SHORT prompt reliably yields a LONG, mechanical, verifiable artifact whose content " +
	"is already fully determined by the input:\n" +
	"  • boilerplate/scaffold generation from a clear spec\n" +
	"  • docstrings/comments for code you paste in full\n" +
	"  • rote refactors following one explicit pattern across many sites\n" +
	"  • commit messages from a diff you include\n" +
	"  • format/data conversion (JSON↔YAML, CSV→struct, etc.) where the input holds all the content\n\n" +
	"DO NOT use it when the prompt would have to encode as much reasoning/design/derivation as the " +
	"output itself — that thinking cost stays on YOU regardless, so delegating just adds a lossy " +
	"round-trip. Bad fits: writing precise test cases, edge-case enumeration, anything needing " +
	"precomputed exact values, open-ended design, or judgement calls.\n\n" +
	"HARD LIMITS: 64k input context, 8k output tokens. Hypatia CANNOT explore, read files, ask " +
	"clarifying questions, or spawn subagents — the task must arrive COMPLETE and UNAMBIGUOUS in the " +
	"single `prompt`, with every bit of needed context/content already inlined. Returns Hypatia's raw text."

// registerHypatiaTool wires the delegate_to_hypatia tool onto srv. Config comes
// from the environment (following the token_env convention): HYPATIA_API_KEY is
// required; HYPATIA_URL and HYPATIA_MODEL optionally override the defaults.
func registerHypatiaTool(srv *channel.Server) {
	url := os.Getenv("HYPATIA_URL")
	if url == "" {
		url = hypatiaDefaultURL
	}
	model := os.Getenv("HYPATIA_MODEL")
	if model == "" {
		model = hypatiaDefaultModel
	}
	c := &hypatiaClient{
		url:    url,
		apiKey: os.Getenv("HYPATIA_API_KEY"),
		model:  model,
		http:   &http.Client{Timeout: hypatiaTimeout},
	}

	srv.RegisterTool(mcp.Tool{
		Name:        "delegate_to_hypatia",
		Description: hypatiaDescription,
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"prompt": map[string]any{
					"type":        "string",
					"description": "The COMPLETE, self-contained task with all needed context inlined. Hypatia sees only this.",
				},
				"system_prompt": map[string]any{
					"type":        "string",
					"description": "Optional framing/role for the request (e.g. \"You output only code, no prose.\"). Keep it terse.",
				},
			},
			"required": []string{"prompt"},
		},
		Handler: func(ctx context.Context, args json.RawMessage) (string, error) {
			var a struct {
				Prompt       string `json:"prompt"`
				SystemPrompt string `json:"system_prompt"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return "", err
			}
			if strings.TrimSpace(a.Prompt) == "" {
				return "", errors.New("prompt is required and must be non-empty")
			}
			return c.generate(ctx, a.Prompt, a.SystemPrompt)
		},
	})
}
