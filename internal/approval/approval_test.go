package approval

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestApprovalFlow(t *testing.T) {
	m := NewManager("http://tailnet.example")
	reqSrv := httptest.NewServer(m.RequestHandler())
	defer reqSrv.Close()
	appSrv := httptest.NewServer(m.ApproveHandler())
	defer appSrv.Close()

	// Create a request.
	resp, err := http.PostForm(reqSrv.URL+"/request", url.Values{"desc": {"restart relayd"}})
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	var created struct{ Token, Link string }
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if created.Token == "" {
		t.Fatal("expected non-empty token")
	}

	// Status should be pending.
	statusResp, err := http.Get(reqSrv.URL + "/status/" + created.Token)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	var st struct{ Status string }
	_ = json.NewDecoder(statusResp.Body).Decode(&st)
	statusResp.Body.Close()
	if st.Status != "pending" {
		t.Fatalf("status = %q, want pending", st.Status)
	}

	// Approve via the tailnet-side handler.
	approveResp, err := http.PostForm(appSrv.URL+"/approve/"+created.Token, url.Values{"decision": {"approve"}})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	approveResp.Body.Close()

	// Status should now be approved.
	statusResp2, err := http.Get(reqSrv.URL + "/status/" + created.Token)
	if err != nil {
		t.Fatalf("status2: %v", err)
	}
	var st2 struct{ Status string }
	_ = json.NewDecoder(statusResp2.Body).Decode(&st2)
	statusResp2.Body.Close()
	if st2.Status != "approved" {
		t.Fatalf("status = %q, want approved", st2.Status)
	}

	// A second decision on the same token must not flip it.
	approveResp2, err := http.PostForm(appSrv.URL+"/approve/"+created.Token, url.Values{"decision": {"deny"}})
	if err != nil {
		t.Fatalf("second approve: %v", err)
	}
	approveResp2.Body.Close()
	statusResp3, _ := http.Get(reqSrv.URL + "/status/" + created.Token)
	var st3 struct{ Status string }
	_ = json.NewDecoder(statusResp3.Body).Decode(&st3)
	statusResp3.Body.Close()
	if st3.Status != "approved" {
		t.Fatalf("status changed after second decision: %q", st3.Status)
	}
}

func TestConsumeOneShot(t *testing.T) {
	m := NewManager("http://tailnet.example")
	token, _ := m.create("delete message 42", "del:42", time.Minute)

	m.mu.Lock()
	m.pending[token].status = StatusApproved
	m.mu.Unlock()

	st, ok := m.Consume(token, "del:42")
	if !ok || st != StatusApproved {
		t.Fatalf("first consume: status=%q ok=%v, want approved/true", st, ok)
	}

	// A second consume of the same approval must not succeed - one
	// approval authorizes exactly one action.
	st2, ok2 := m.Consume(token, "del:42")
	if !ok2 || st2 == StatusApproved {
		t.Fatalf("second consume: status=%q ok=%v, want non-approved/true (replay must be rejected)", st2, ok2)
	}
}

func TestConsumeActionMismatch(t *testing.T) {
	m := NewManager("http://tailnet.example")
	token, _ := m.create("delete message 42", "del:42", time.Minute)

	m.mu.Lock()
	m.pending[token].status = StatusApproved
	m.mu.Unlock()

	// Approval was bound to del:42 - presenting a different action hash
	// (e.g. an attacker substituting a different target) must be rejected.
	st, ok := m.Consume(token, "del:99")
	if !ok || st == StatusApproved {
		t.Fatalf("mismatched consume: status=%q ok=%v, want non-approved/true", st, ok)
	}

	// The original action hash must still work - the mismatched attempt
	// above must not have consumed or corrupted the token.
	st2, ok2 := m.Consume(token, "del:42")
	if !ok2 || st2 != StatusApproved {
		t.Fatalf("correct consume after mismatch: status=%q ok=%v, want approved/true", st2, ok2)
	}
}

func TestConsumeUnboundToken(t *testing.T) {
	// Tokens created via the plain Create (empty actionHash) accept any
	// actionHash at Consume time - existing unbound callers keep working.
	m := NewManager("http://tailnet.example")
	token, _ := m.create("legacy caller", "", time.Minute)

	m.mu.Lock()
	m.pending[token].status = StatusApproved
	m.mu.Unlock()

	st, ok := m.Consume(token, "anything")
	if !ok || st != StatusApproved {
		t.Fatalf("unbound consume: status=%q ok=%v, want approved/true", st, ok)
	}
}

func TestApprovalExpiry(t *testing.T) {
	m := NewManager("http://tailnet.example")
	token, _ := m.create("expires fast", "", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	req, ok := m.get(token)
	if !ok {
		t.Fatal("expected token to still be tracked (not yet gc'd)")
	}
	if req.status != StatusExpired {
		t.Fatalf("status = %q, want expired", req.status)
	}
}

func TestUnknownToken(t *testing.T) {
	m := NewManager("http://tailnet.example")
	appSrv := httptest.NewServer(m.ApproveHandler())
	defer appSrv.Close()
	resp, err := http.Get(appSrv.URL + "/approve/doesnotexist")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
