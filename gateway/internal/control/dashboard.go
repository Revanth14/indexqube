package control

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	dashboardCookieName = "indexqube_dashboard"
	dashboardTicketTTL  = time.Minute
	dashboardSessionTTL = 12 * time.Hour
	dashboardCSRFHeader = "X-IndexQube-Dashboard"
)

type dashboardSessions struct {
	mu       sync.Mutex
	tickets  map[string]dashboardGrant
	sessions map[string]dashboardGrant
	now      func() time.Time
}

type dashboardGrant struct {
	expires   time.Time
	workspace string
}

func newDashboardSessions() *dashboardSessions {
	return &dashboardSessions{
		tickets: make(map[string]dashboardGrant), sessions: make(map[string]dashboardGrant), now: time.Now,
	}
}

func (d *dashboardSessions) issueTicket(workspace string) (string, error) {
	ticket, err := randomDashboardCredential()
	if err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cleanupLocked()
	d.tickets[ticket] = dashboardGrant{expires: d.now().Add(dashboardTicketTTL), workspace: workspace}
	return ticket, nil
}

func (d *dashboardSessions) exchangeTicket(ticket string) (string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cleanupLocked()
	grant, ok := d.tickets[ticket]
	if !ok || !grant.expires.After(d.now()) {
		return "", false, nil
	}
	delete(d.tickets, ticket)
	session, err := randomDashboardCredential()
	if err != nil {
		return "", false, err
	}
	grant.expires = d.now().Add(dashboardSessionTTL)
	d.sessions[session] = grant
	return session, true, nil
}

func (d *dashboardSessions) validSession(session string) bool {
	if strings.TrimSpace(session) == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cleanupLocked()
	grant, ok := d.sessions[session]
	return ok && grant.expires.After(d.now())
}

func (d *dashboardSessions) workspace(session string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cleanupLocked()
	grant, ok := d.sessions[session]
	if !ok || !grant.expires.After(d.now()) {
		return "", false
	}
	return grant.workspace, true
}

func (d *dashboardSessions) cleanupLocked() {
	now := d.now()
	for credential, grant := range d.tickets {
		if !grant.expires.After(now) {
			delete(d.tickets, credential)
		}
	}
	for credential, grant := range d.sessions {
		if !grant.expires.After(now) {
			delete(d.sessions, credential)
		}
	}
}

func randomDashboardCredential() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate dashboard credential: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (h *Handler) createDashboardSession(w http.ResponseWriter, r *http.Request) {
	// Browser tickets can only be minted by a bearer-authenticated local client;
	// a browser session cannot mint another browser session.
	if !authenticate(h.token, r) {
		writeUnauthorized(w)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	var body struct {
		Workspace string `json:"workspace"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	body.Workspace = strings.TrimSpace(body.Workspace)
	if body.Workspace == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", errors.New("dashboard workspace is required"))
		return
	}
	ticket, err := h.dashboard.issueTicket(body.Workspace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "dashboard_state_error", err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"url": "http://" + r.Host + "/control/ui/?ticket=" + url.QueryEscape(ticket),
	})
}

func (h *Handler) dashboardContext(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(dashboardCookieName)
	if err != nil {
		writeUnauthorized(w)
		return
	}
	workspace, ok := h.dashboard.workspace(cookie.Value)
	if !ok {
		writeUnauthorized(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"workspace": workspace})
}

func (h *Handler) serveDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", errors.New("dashboard only supports GET"))
		return
	}
	if ticket := strings.TrimSpace(r.URL.Query().Get("ticket")); ticket != "" {
		if r.URL.Path != "/control/ui/" && r.URL.Path != "/control/ui" {
			writeUnauthorized(w)
			return
		}
		session, ok, err := h.dashboard.exchangeTicket(ticket)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "dashboard_state_error", err)
			return
		}
		if !ok {
			writeUnauthorized(w)
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name: dashboardCookieName, Value: session, Path: "/control", HttpOnly: true,
			SameSite: http.SameSiteStrictMode, MaxAge: int(dashboardSessionTTL.Seconds()),
		})
		http.Redirect(w, r, "/control/ui/", http.StatusSeeOther)
		return
	}
	if !h.authenticateDashboard(r) {
		writeUnauthorized(w)
		return
	}
	switch r.URL.Path {
	case "/control/ui", "/control/ui/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(dashboardHTML))
	case "/control/ui/app.js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = w.Write([]byte(dashboardJS))
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) authenticateDashboard(r *http.Request) bool {
	cookie, err := r.Cookie(dashboardCookieName)
	return err == nil && h.dashboard.validSession(cookie.Value)
}

func validDashboardMutation(r *http.Request) bool {
	if r.Header.Get(dashboardCSRFHeader) != "1" {
		return false
	}
	origin := strings.TrimRight(strings.TrimSpace(r.Header.Get("Origin")), "/")
	return origin != "" && origin == "http://"+r.Host
}
