# 07 — Pre-Signed URLs & Auth

**Code:** `internal/presign/`, `internal/gateway/auth.go`, `presign_handler.go`

## What a pre-signed URL is

A time-limited link that grants **one specific operation** (GET or PUT this
bucket/key) **without credentials**. You generate it once with your secret; you
can then hand the raw URL to a browser, a mobile app, or a third party, and they
can use it until it expires — no login, no access key.

Example: a photo app's backend mints a 15-minute pre-signed PUT URL and returns
it to the browser, which uploads directly to StorageMesh. The browser never sees
the secret key.

## How it works: HMAC-SHA256

The server holds a secret. To sign, it builds a canonical string and HMACs it:

```
stringToSign = "METHOD\nBUCKET\nKEY\nEXPIRES"
signature    = hex( HMAC-SHA256(secret, stringToSign) )
url          = /bucket/key?expires=<ts>&signature=<sig>
```

To verify an incoming request, the server recomputes the signature from the
request's own method/bucket/key and the `expires` in the query, and checks it
matches and hasn't expired.

**Why HMAC, not a plain hash?** A plain `SHA-256(method|bucket|key|expires)`
could be produced by anyone — they'd just hash the same public inputs and forge
a URL. HMAC mixes in the *secret*, so only a party holding the secret can produce
a valid signature. Clients only ever see signatures the server made.

## Two security details in `Verify`

1. **Expiry is enforced.** A leaked URL stops working after its window closes.
   `Sign` with a negative duration produces an already-expired URL (used in tests).

2. **Constant-time comparison.** The signature check uses `hmac.Equal`, not `==`.
   A naive string compare returns as soon as two bytes differ, and that timing
   difference leaks *how many* leading bytes were correct — enough for an
   attacker to recover a valid signature byte-by-byte over many requests.
   Constant-time compare takes the same time regardless, closing the side
   channel. This is a classic, real vulnerability class.

Including the **method** in the signed string matters too: it stops a GET
signature from being replayed as a PUT. `TestVerifyRejectsTamper` checks exactly
that.

## Minting is privileged

`GET /bucket/key?presign=GET&expiry=900` mints a URL. Minting **requires the
real access key** (checked constant-time in `handleGeneratePresign`) — a
pre-signed URL is a bearer capability, so we must not let a leaked URL be used to
mint *more* URLs and escalate into unlimited access.

## Auth model & scope

`auth.go` implements a simple shared-access-key check (`X-Access-Key` header,
constant-time compared). Object routes accept **either** that key **or** a valid
pre-signed signature, which is why a browser following a pre-signed link with no
header still works (`authorizeObject`).

**Scope note / deviation from real S3:** production S3 uses AWS Signature V4 —
the client signs a canonical form of the *whole* request per call. We use a
shared key plus full HMAC pre-signed URLs, which demonstrates the signing
mechanic while keeping the focus on the storage system. Swapping in SigV4 would
touch only `auth.go`. See decision **D-10**.

## What the test proves

`TestPresignedURLFlow` (in `gateway_test.go`) runs the full loop with auth on:

- an unauthenticated GET is `403`,
- minting a pre-signed URL requires the access key,
- the returned URL then works with **no credentials at all**.
