package events

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher writes events to a Kafka topic. It uses the object key as the
// partition key so all events for one object land on the same partition and are
// therefore delivered in order (created before deleted). Ordering across
// different objects is not guaranteed, and doesn't need to be.
type KafkaPublisher struct {
	writer *kafka.Writer
	log    *slog.Logger
}

// NewKafkaPublisher connects a writer to the given brokers and topic. kafka-go
// dials lazily on first write, so this does not fail if the broker is briefly
// unavailable at startup.
func NewKafkaPublisher(brokers []string, topic string, log *slog.Logger) *KafkaPublisher {
	if log == nil {
		log = slog.Default()
	}
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{}, // hash the message key -> stable partition
			// Async keeps Publish from blocking the request path; delivery errors
			// surface via the completion callback below, logged not fatal.
			Async: true,
			Completion: func(msgs []kafka.Message, err error) {
				if err != nil {
					log.Warn("kafka delivery failed", "err", err, "count", len(msgs))
				}
			},
		},
		log: log,
	}
}

// Publish serializes the event and enqueues it. Because the writer is Async and
// events are best-effort, this returns quickly and never blocks the user's
// request; a broker outage degrades to lost events, not failed uploads.
func (k *KafkaPublisher) Publish(ctx context.Context, e Event) error {
	payload, err := Marshal(e)
	if err != nil {
		return err
	}
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.Bucket + "/" + e.Key),
		Value: payload,
	})
}

func (k *KafkaPublisher) Close() error { return k.writer.Close() }
