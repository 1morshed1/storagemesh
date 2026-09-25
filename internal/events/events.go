// Package events publishes object-lifecycle events (created, deleted,
// lifecycle transitions) to a stream so downstream consumers — analytics,
// billing, CDN cache invalidation — can react without the gateway knowing about
// them. This is the classic decoupling win of event streaming: producers and
// consumers evolve independently.
//
// The gateway depends on the Publisher interface, not on Kafka. That gives us:
//   - NoopPublisher for tests and single-node local runs (no broker needed),
//   - KafkaPublisher for real deployments,
// swappable by config. Crucially, a publish failure must NEVER fail the user's
// upload: the bytes are already durably stored and metadata committed by the
// time we publish. Events are best-effort side channels, so publish errors are
// logged, not returned to the client.
package events

import (
	"context"
	"encoding/json"
	"time"
)

// EventType enumerates the lifecycle events we emit.
type EventType string

const (
	ObjectCreated EventType = "object.created"
	ObjectDeleted EventType = "object.deleted"
	ObjectRestored EventType = "object.restored"
)

// Event is the payload published to the stream. Key fields let consumers filter
// without fetching metadata.
type Event struct {
	Type      EventType         `json:"type"`
	Bucket    string            `json:"bucket"`
	Key       string            `json:"key"`
	VersionID string            `json:"version_id,omitempty"`
	Size      int64             `json:"size,omitempty"`
	ETag      string            `json:"etag,omitempty"`
	Time      time.Time         `json:"time"`
	Extra     map[string]string `json:"extra,omitempty"`
}

// Publisher is the event-sink contract. Publish should be non-blocking-ish and
// must be safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
	Close() error
}

// Marshal is shared by implementations to serialize an event.
func Marshal(e Event) ([]byte, error) {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	return json.Marshal(e)
}

// NoopPublisher discards events. Used when no broker is configured. It keeps the
// gateway code path identical whether or not Kafka is wired up.
type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, Event) error { return nil }
func (NoopPublisher) Close() error                         { return nil }
