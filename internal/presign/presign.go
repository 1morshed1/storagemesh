// Package presign generates and verifies pre-signed URLs: time-limited,
// credential-free links to PUT or GET an object.
//
// The idea: the server holds a secret key. To grant temporary access it signs
// (method, bucket, key, expiry) with HMAC-SHA256 and appends the signature to a
// URL. Anyone holding that URL can perform exactly that one operation until it
// expires — no account, no credentials. The server re-computes the signature on
// each request and only serves it if it matches and hasn't expired.
//
// Why HMAC and not a plain hash: a plain SHA-256 of the parameters could be
// forged by anyone (they'd just hash the same string). HMAC mixes in the secret
// key, so only a party holding the secret can produce a valid signature. The
// client never sees the key — only signatures the server produced.
package presign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

var (
	// ErrExpired means the URL's expiry timestamp is in the past.
	ErrExpired = errors.New("presign: url expired")
	// ErrBadSignature means the signature does not match (tampered or forged).
	ErrBadSignature = errors.New("presign: invalid signature")
	// ErrMalformed means required query params are missing or unparsable.
	ErrMalformed = errors.New("presign: malformed presigned request")
)

// Signer signs and verifies pre-signed URLs using a shared secret.
type Signer struct {
	secret []byte
}

// New returns a Signer keyed by secret. The secret must stay server-side; anyone
// who has it can mint valid URLs.
func New(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

// stringToSign is the canonical string both signing and verification hash. The
// exact byte layout must match on both sides, so it lives in one function. We
// include the method so a GET signature can't be replayed as a PUT.
func stringToSign(method, bucket, key string, expires int64) string {
	return fmt.Sprintf("%s\n%s\n%s\n%d", method, bucket, key, expires)
}

// sign computes the hex HMAC-SHA256 signature for the canonical string.
func (s *Signer) sign(method, bucket, key string, expires int64) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(stringToSign(method, bucket, key, expires)))
	return hex.EncodeToString(mac.Sum(nil))
}

// Sign returns the query string ("expires=...&signature=...") granting `method`
// access to bucket/key until now+expiry. The caller appends it to the object URL.
func (s *Signer) Sign(method, bucket, key string, expiry time.Duration) string {
	expires := time.Now().Add(expiry).Unix()
	sig := s.sign(method, bucket, key, expires)
	q := url.Values{}
	q.Set("expires", strconv.FormatInt(expires, 10))
	q.Set("signature", sig)
	return q.Encode()
}

// SignURL returns a full pre-signed URL for baseURL + /bucket/key.
func (s *Signer) SignURL(baseURL, method, bucket, key string, expiry time.Duration) string {
	return fmt.Sprintf("%s/%s/%s?%s", baseURL, bucket, url.PathEscape(key), s.Sign(method, bucket, key, expiry))
}

// Verify checks a pre-signed request. It confirms the signature matches and the
// URL has not expired.
//
// Two security details:
//   - Expiry is checked, so a leaked URL stops working after its window.
//   - The signature comparison uses hmac.Equal (constant time). A naive `==`
//     compare leaks timing information that lets an attacker recover a valid
//     signature byte-by-byte. Constant-time compare closes that side channel.
func (s *Signer) Verify(method, bucket, key string, query url.Values) error {
	expStr := query.Get("expires")
	gotSig := query.Get("signature")
	if expStr == "" || gotSig == "" {
		return ErrMalformed
	}
	expires, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return ErrMalformed
	}
	// Verify the signature FIRST, then expiry. Order matters only for what error
	// we surface; both must pass. We compute the expected signature over the
	// exact same fields the client's URL claims.
	want := s.sign(method, bucket, key, expires)
	if !hmac.Equal([]byte(want), []byte(gotSig)) {
		return ErrBadSignature
	}
	if time.Now().Unix() > expires {
		return ErrExpired
	}
	return nil
}
