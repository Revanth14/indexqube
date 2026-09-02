package control

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestDashboardAssetsKeepCredentialsOutOfJavaScriptAndUseSafeDOMRendering(t *testing.T) {
	for _, forbidden := range []string{"Authorization", "control-auth.json", "innerHTML", "document.write"} {
		if strings.Contains(dashboardJS, forbidden) {
			t.Fatalf("dashboard JavaScript contains forbidden credential/DOM pattern %q", forbidden)
		}
	}
	if !strings.Contains(dashboardJS, "textContent") || !strings.Contains(dashboardHTML, `src="/control/ui/app.js"`) {
		t.Fatal("dashboard assets do not use the external safe-DOM client")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; JavaScript syntax is exercised by browser smoke instead")
	}
	command := exec.Command(nodePath, "--check", "-")
	command.Stdin = strings.NewReader(dashboardJS)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("dashboard JavaScript syntax: %v: %s", err, output)
	}
}

func TestDashboardCredentialsExpireAndTicketsAreOneShot(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	sessions := newDashboardSessions()
	sessions.now = func() time.Time { return now }
	ticket, err := sessions.issueTicket("/repo")
	if err != nil {
		t.Fatal(err)
	}
	session, ok, err := sessions.exchangeTicket(ticket)
	if err != nil || !ok || !sessions.validSession(session) {
		t.Fatalf("session=%q ok=%v err=%v", session, ok, err)
	}
	if _, replayed, err := sessions.exchangeTicket(ticket); err != nil || replayed {
		t.Fatalf("replayed=%v err=%v", replayed, err)
	}
	if workspace, found := sessions.workspace(session); !found || workspace != "/repo" {
		t.Fatalf("workspace=%q found=%v", workspace, found)
	}
	now = now.Add(dashboardSessionTTL + time.Second)
	if sessions.validSession(session) {
		t.Fatal("expired dashboard session remained valid")
	}
	ticket, err = sessions.issueTicket("/repo")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(dashboardTicketTTL + time.Second)
	if _, ok, err := sessions.exchangeTicket(ticket); err != nil || ok {
		t.Fatalf("expired ticket ok=%v err=%v", ok, err)
	}
}
