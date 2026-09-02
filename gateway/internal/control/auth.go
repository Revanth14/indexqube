package control

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

const (
	bearerScheme       = "Bearer"
	AuthContractHeader = "X-IndexQube-Control-Auth"
	AuthContractValue  = "bearer-v1"
)

func authenticate(token string, r *http.Request) bool {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	scheme, credential, ok := strings.Cut(strings.TrimSpace(values[0]), " ")
	if !ok || !strings.EqualFold(scheme, bearerScheme) || credential == "" || strings.ContainsAny(credential, " \t\r\n") {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(credential), []byte(token)) == 1
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="indexqube-control"`)
	writeError(w, http.StatusUnauthorized, "unauthorized", errors.New("valid control API bearer token required"))
}
