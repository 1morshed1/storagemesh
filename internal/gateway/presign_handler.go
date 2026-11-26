package gateway

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"time"
)

// handleGeneratePresign mints a pre-signed URL for (method, bucket, key). It is
// reached via GET /bucket/key?presign=GET&expiry=900.
//
// Security: minting a pre-signed URL is itself a privileged action — the URL is
// a bearer token that grants access without credentials — so we require the
// access key here, checked in constant time. We do NOT accept a pre-signed
// signature as authorization to mint more URLs (that would let a leaked URL
// escalate into unlimited access).
func (s *Server) handleGeneratePresign(w http.ResponseWriter, r *http.Request, bucket, key, method string) {
	if s.accessKey != "" {
		got := r.Header.Get("X-Access-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.accessKey)) != 1 {
			writeError(w, http.StatusForbidden, "credentials required to mint a pre-signed URL")
			return
		}
	}

	expiry := 15 * time.Minute
	if v := r.URL.Query().Get("expiry"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			expiry = time.Duration(secs) * time.Second
		}
	}

	query := s.signer.Sign(method, bucket, key, expiry)
	writeJSON(w, http.StatusOK, map[string]any{
		"method":     method,
		"bucket":     bucket,
		"key":        key,
		"expires_in": int(expiry.Seconds()),
		// The caller appends this query string to the object URL.
		"url": "/" + bucket + "/" + key + "?" + query,
	})
}
