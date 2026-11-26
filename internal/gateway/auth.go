package gateway

import (
	"crypto/subtle"
	"net/http"

	"github.com/1morshed1/storagemesh/internal/presign"
)

// auth wraps a handler with the shared-access-key check used for normal
// (non-presigned) requests. The client sends the key in the X-Access-Key header;
// we compare it in constant time.
//
// NOTE ON SCOPE: real S3 uses AWS Signature V4 — the client signs a canonical
// form of the whole request with a secret, and the server re-derives the
// signature. We implement a simpler shared-key check here to keep the focus on
// the storage system, plus full HMAC pre-signed URLs (see presign package) which
// demonstrate the signing mechanic. Swapping in SigV4 would touch only this file.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.accessKey == "" {
			next(w, r) // auth disabled (local/dev)
			return
		}
		got := r.Header.Get("X-Access-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.accessKey)) != 1 {
			writeError(w, http.StatusForbidden, "invalid or missing access key")
			return
		}
		next(w, r)
	}
}

// authorizeObject authorizes an object-level request via EITHER a valid
// pre-signed URL (query has expires+signature) OR the shared access key. This is
// why PutObject/GetObject register without the auth() wrapper: they may be hit
// by a browser following a pre-signed link that carries no header.
func (s *Server) authorizeObject(r *http.Request, bucket, key string) bool {
	q := r.URL.Query()
	if q.Get("signature") != "" {
		// Pre-signed path: the signature itself is the credential.
		return s.signer.Verify(r.Method, bucket, key, q) == nil
	}
	if s.accessKey == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Access-Key")), []byte(s.accessKey)) == 1
}

// presignErrorStatus maps a presign verification error to an HTTP status for
// clearer client feedback.
func presignErrorStatus(err error) int {
	switch err {
	case presign.ErrExpired:
		return http.StatusForbidden
	default:
		return http.StatusForbidden
	}
}
