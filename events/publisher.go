// Package events gives the services one single way to publish and consume
// messages, whatever the transport is. A channel is identified by its
// prefix — "kafka:order-events" or "rabbitmq:sms.send" — and it is this
// library that decides which way it goes; neither the emitter nor the
// service knows.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

// splitChannel separates the transport from the channel name.
// It returns an empty transport when the channel carries no prefix, which is
// a declaration error in system.yaml and not something to guess at here.
func splitChannel(channel string) (transport, name string) {
	parts := strings.SplitN(channel, ":", 2)
	if len(parts) != 2 {
		return "", channel
	}
	return parts[0], parts[1]
}

// Publisher publishes messages on the channels the service declares.
type Publisher struct {
	serviceName string
	mu          sync.Mutex
	writers     map[string]*kafka.Writer
}

// NewPublisher creates the publisher of the service. It does not connect
// yet: the connection is opened on the first publish of each channel, so
// startup is not blocked when the broker is not ready yet.
func NewPublisher(serviceName string) *Publisher {
	return &Publisher{serviceName: serviceName, writers: map[string]*kafka.Writer{}}
}

// Publish sends the payload to the given channel, serialized as JSON.
func (p *Publisher) Publish(ctx context.Context, channel string, payload any) error {
	transport, name := splitChannel(channel)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serializing the message for %s: %w", channel, err)
	}

	switch transport {
	case "kafka":
		return p.publishKafka(ctx, name, body)
	case "rabbitmq":
		return publishRabbit(ctx, name, body)
	default:
		return fmt.Errorf("channel %q has no recognized transport", channel)
	}
}

func (p *Publisher) publishKafka(ctx context.Context, topic string, body []byte) error {
	p.mu.Lock()
	w, ok := p.writers[topic]
	if !ok {
		w = &kafka.Writer{
			Addr:                   kafka.TCP(brokers()...),
			Topic:                  topic,
			AllowAutoTopicCreation: true,
		}
		p.writers[topic] = w
	}
	p.mu.Unlock()

	return w.WriteMessages(ctx, kafka.Message{Value: body})
}

func publishRabbit(ctx context.Context, queue string, body []byte) error {
	conn, err := amqpDial()
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("opening rabbitmq channel: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring queue %s: %w", queue, err)
	}
	return ch.PublishWithContext(ctx, "", queue, false, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
}

// Close closes the writers that are open.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.writers {
		_ = w.Close()
	}
	return nil
}

func brokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		raw = "kafka:9092"
	}
	return strings.Split(raw, ",")
}

func amqpDial() (*amqp.Connection, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://tt:tt@rabbitmq:5672"
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connecting to rabbitmq at %s: %w", url, err)
	}
	return conn, nil
}
