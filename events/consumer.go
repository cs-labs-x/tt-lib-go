package events

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

// retryDelay separates two attempts of a consume loop when the broker is not
// ready yet or the session drops. It holds for BOTH transports: until now
// only RabbitMQ honoured it and the Kafka loop retried back to back, with no
// wait at all. There is no growing backoff (noted down as a pending
// improvement) — a fixed value is enough to keep startup unblocked, which is
// what the fix asked for, and it is the same value and the same criterion as
// RETRY_DELAY_MS in tt-lib-node and _RETRY_DELAY_SECONDS in tt-lib-py.
const retryDelay = 2 * time.Second

// Handler processes a received message. The channel arrives with its prefix,
// so the handler knows where it came from without the consumer having to
// tell it separately.
type Handler func(ctx context.Context, channel string, payload []byte) error

// Consumer listens on the channels the service declares.
type Consumer struct {
	serviceName string
	channels    []string
	handle      Handler
	readers     []*kafka.Reader
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewConsumer creates the consumer of the service.
func NewConsumer(serviceName string, channels []string, h Handler) *Consumer {
	return &Consumer{serviceName: serviceName, channels: channels, handle: h}
}

// Start starts one loop per channel and returns immediately. It does not
// block the startup of the service: if a broker is not ready, that loop
// retries on its own while the rest of the service serves requests.
//
// Both transports retry, but NOT along the same path, and the previous
// comment ("Kafka or RabbitMQ, both behave the same") took that for granted
// just when it was false: the Kafka loop retried back to back, with no wait.
// Today they do wait the same (retryDelay), even though the mechanism is
// still different and it helps to know that when reading the logs: kafka-go
// reconnects internally and the loop only calls ReadMessage again, while
// RabbitMQ dials the connection, the channel and the Consume from scratch on
// every attempt.
//
// The channels are validated BEFORE any loop starts: if one of them does not
// carry a recognized transport, Start returns the error without having
// launched a single goroutine, so the caller does not have to call Close()
// to clean up a half-done startup.
func (c *Consumer) Start(ctx context.Context) error {
	for _, channel := range c.channels {
		if transport, _ := splitChannel(channel); transport != "kafka" && transport != "rabbitmq" {
			return fmt.Errorf("channel %q has no recognized transport", channel)
		}
	}

	ctx, c.cancel = context.WithCancel(ctx)
	for _, channel := range c.channels {
		transport, name := splitChannel(channel)
		switch transport {
		case "kafka":
			c.startKafka(ctx, channel, name)
		case "rabbitmq":
			c.startRabbit(ctx, channel, name)
		}
	}
	return nil
}

func (c *Consumer) startKafka(ctx context.Context, channel, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers(),
		Topic:   topic,
		GroupID: c.serviceName,
	})
	c.readers = append(c.readers, r)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				log.Printf("%s: reading from %s: %v", c.serviceName, channel, err)
				// The wait is NOT optional: this `continue` was the only
				// retry with no rate limit in the three libraries, and with
				// the broker down it spun in a tight loop filling the log
				// — a failure mode already seen while verifying the phase,
				// with GroupCoordinatorNotAvailable. It is the same
				// cancellable wait startRabbit uses, so it does not delay
				// the graceful shutdown either.
				if !sleepOrDone(ctx, retryDelay) {
					return // cancelled while waiting to retry
				}
				continue
			}
			if err := c.handle(ctx, channel, m.Value); err != nil {
				log.Printf("%s: processing %s: %v", c.serviceName, channel, err)
			}
		}
	}()
}

// startRabbit launches the channel loop and returns immediately, just like
// startKafka: the connection, the AMQP channel, the queue declaration and
// the Consume happen INSIDE the goroutine, not before launching it. If the
// broker is not ready (or the connection drops later), the loop retries every
// retryDelay instead of taking down the startup of the service or going
// dead — before this fix those steps were synchronous inside Start(), so a
// slow broker stopped the whole service from starting.
func (c *Consumer) startRabbit(ctx context.Context, channel, queue string) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if ctx.Err() != nil {
				return // graceful shutdown, do not even try
			}

			conn, ch, deliveries, err := c.connectRabbit(queue)
			if err != nil {
				log.Printf("%s: connecting to rabbitmq for %s: %v", c.serviceName, channel, err)
				if !sleepOrDone(ctx, retryDelay) {
					return // cancelled while waiting to retry
				}
				continue
			}

			c.consumeRabbit(ctx, channel, deliveries)
			_ = ch.Close()
			_ = conn.Close()
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			// If we get here, consumeRabbit returned because the delivery
			// channel closed (connection lost) and not because the context
			// was cancelled: try the connection again from the start.
		}
	}()
}

// connectRabbit opens the connection, declares the queue and starts the
// Consume. It lives apart from startRabbit so the retry loop can call it over
// and over without duplicating the cleanup at every failure point.
func (c *Consumer) connectRabbit(queue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	conn, err := amqpDial()
	if err != nil {
		return nil, nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("opening rabbitmq channel: %w", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("declaring queue %s: %w", queue, err)
	}
	deliveries, err := ch.Consume(queue, c.serviceName, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("consuming from %s: %w", queue, err)
	}
	return conn, ch, deliveries, nil
}

// consumeRabbit forwards every delivery to the handler until the context is
// cancelled (graceful shutdown) or the delivery channel closes (connection
// lost, a reconnect is needed). The caller tells the two cases apart with
// ctx.Err() on return.
func (c *Consumer) consumeRabbit(ctx context.Context, channel string, deliveries <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			if err := c.handle(ctx, channel, d.Body); err != nil {
				log.Printf("%s: processing %s: %v", c.serviceName, channel, err)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

// sleepOrDone waits d, or returns earlier if the context is cancelled — so
// that a retry wait does not delay the graceful shutdown of the consumer. It
// returns false when the return was caused by cancellation, not by the wait
// having finished.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// Close stops the loops and waits for them to finish. There is no need to
// close RabbitMQ connections separately: every startRabbit goroutine closes
// its own connection/channel as soon as consumeRabbit returns, and it returns
// right away when the context is cancelled (or during the retry wait, through
// sleepOrDone) — so waiting with wg.Wait() already waits for that cleanup to
// finish.
func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	for _, r := range c.readers {
		_ = r.Close()
	}
	c.wg.Wait()
	return nil
}
