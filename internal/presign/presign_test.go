package presign

import (
	"net/url"
	"testing"
	"time"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	s := New("super-secret")
	q := s.Sign("GET", "photos", "a/b.jpg", time.Hour)
	vals, _ := url.ParseQuery(q)
	if err := s.Verify("GET", "photos", "a/b.jpg", vals); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyRejectsTamper(t *testing.T) {
	s := New("secret")
	q := s.Sign("GET", "b", "k", time.Hour)
	vals, _ := url.ParseQuery(q)

	// Tamper with the key the request targets: signature no longer matches.
	if err := s.Verify("GET", "b", "different-key", vals); err != ErrBadSignature {
		t.Fatalf("want ErrBadSignature, got %v", err)
	}
	// Tamper with the method (GET signature replayed as PUT).
	if err := s.Verify("PUT", "b", "k", vals); err != ErrBadSignature {
		t.Fatalf("method swap should fail, got %v", err)
	}
}

func TestVerifyRejectsExpired(t *testing.T) {
	s := New("secret")
	// Sign with a negative expiry so it is already in the past.
	q := s.Sign("GET", "b", "k", -time.Minute)
	vals, _ := url.ParseQuery(q)
	if err := s.Verify("GET", "b", "k", vals); err != ErrExpired {
		t.Fatalf("want ErrExpired, got %v", err)
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	q := New("real-secret").Sign("GET", "b", "k", time.Hour)
	vals, _ := url.ParseQuery(q)
	// An attacker without the secret cannot forge a valid signature.
	if err := New("guessed-secret").Verify("GET", "b", "k", vals); err != ErrBadSignature {
		t.Fatalf("want ErrBadSignature with wrong secret, got %v", err)
	}
}

func TestVerifyMalformed(t *testing.T) {
	s := New("secret")
	if err := s.Verify("GET", "b", "k", url.Values{}); err != ErrMalformed {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
}
