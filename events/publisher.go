// Package events da a los servicios una forma única de publicar y consumir
// mensajes, sea cual sea el transporte. Un canal se identifica por su
// prefijo — "kafka:order-events" o "rabbitmq:sms.send" — y es esta librería
// la que decide por dónde va; ni el emisor ni el servicio lo saben.
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

// splitChannel separa el transporte del nombre del canal.
// Devuelve transporte vacío si el canal no lleva prefijo, que es un error
// de declaración en system.yaml y no algo que deba adivinarse aquí.
func splitChannel(channel string) (transport, name string) {
	parts := strings.SplitN(channel, ":", 2)
	if len(parts) != 2 {
		return "", channel
	}
	return parts[0], parts[1]
}

// Publisher publica mensajes en los canales que el servicio declare.
type Publisher struct {
	serviceName string
	mu          sync.Mutex
	writers     map[string]*kafka.Writer
}

// NewPublisher crea el publicador del servicio. No conecta todavía: la
// conexión se abre en la primera publicación de cada canal, para no
// bloquear el arranque si el broker aún no está listo.
func NewPublisher(serviceName string) *Publisher {
	return &Publisher{serviceName: serviceName, writers: map[string]*kafka.Writer{}}
}

// Publish envía el payload al canal indicado, serializado como JSON.
func (p *Publisher) Publish(ctx context.Context, channel string, payload any) error {
	transport, name := splitChannel(channel)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serializando el mensaje de %s: %w", channel, err)
	}

	switch transport {
	case "kafka":
		return p.publishKafka(ctx, name, body)
	case "rabbitmq":
		return publishRabbit(ctx, name, body)
	default:
		return fmt.Errorf("canal %q sin transporte reconocido", channel)
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
		return fmt.Errorf("abriendo canal de rabbitmq: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declarando la cola %s: %w", queue, err)
	}
	return ch.PublishWithContext(ctx, "", queue, false, false,
		amqp.Publishing{ContentType: "application/json", Body: body})
}

// Close cierra los escritores abiertos.
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
		return nil, fmt.Errorf("conectando a rabbitmq en %s: %w", url, err)
	}
	return conn, nil
}
