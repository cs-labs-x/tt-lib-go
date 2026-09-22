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

// retryDelay separa dos intentos de un bucle de consumo cuando el broker aún
// no está listo o la sesión se cae. Vale para los DOS transportes: hasta
// ahora solo lo respetaba RabbitMQ y el bucle de Kafka reintentaba a pelo,
// sin espera ninguna. No hay backoff creciente (queda anotado como mejora
// pendiente) — un valor fijo basta para no bloquear el arranque, que es lo
// que pedía la corrección, y es el mismo valor y el mismo criterio que
// RETRY_DELAY_MS en tt-lib-node y _RETRY_DELAY_SECONDS en tt-lib-py.
const retryDelay = 2 * time.Second

// Handler procesa un mensaje recibido. El canal llega con su prefijo, para
// que el manejador sepa de dónde vino sin que el consumidor tenga que
// contarlo aparte.
type Handler func(ctx context.Context, channel string, payload []byte) error

// Consumer escucha los canales que el servicio declare.
type Consumer struct {
	serviceName string
	channels    []string
	handle      Handler
	readers     []*kafka.Reader
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewConsumer crea el consumidor del servicio.
func NewConsumer(serviceName string, channels []string, h Handler) *Consumer {
	return &Consumer{serviceName: serviceName, channels: channels, handle: h}
}

// Start arranca un bucle por canal y devuelve inmediatamente. No bloquea el
// arranque del servicio: si un broker no está listo, ese bucle reintenta por
// su cuenta mientras el resto del servicio atiende peticiones.
//
// Los dos transportes reintentan, pero NO por el mismo camino, y el
// comentario anterior ("Kafka o RabbitMQ, los dos se comportan igual") lo
// daba por sentado justo cuando era falso: el bucle de Kafka reintentaba
// pegado, sin espera. Hoy sí esperan lo mismo (retryDelay), aunque el
// mecanismo siga siendo distinto y convenga saberlo al leer los logs:
// kafka-go reconecta por dentro y el bucle solo vuelve a llamar a
// ReadMessage, mientras que RabbitMQ vuelve a marcar la conexión, el canal y
// el Consume desde cero en cada intento.
//
// Los canales se validan ANTES de arrancar ningún bucle: si uno de ellos no
// trae un transporte reconocido, Start devuelve el error sin haber lanzado
// ninguna goroutine, así el llamador no tiene que invocar Close() para
// limpiar un arranque a medias.
func (c *Consumer) Start(ctx context.Context) error {
	for _, channel := range c.channels {
		if transport, _ := splitChannel(channel); transport != "kafka" && transport != "rabbitmq" {
			return fmt.Errorf("canal %q sin transporte reconocido", channel)
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
					return // parada ordenada
				}
				log.Printf("%s: leyendo de %s: %v", c.serviceName, channel, err)
				// La espera NO es opcional: este `continue` era el único
				// reintento sin límite de tasa de las tres librerías, y con
				// el broker caído giraba en bucle apretado llenando el log
				// —modo de fallo ya observado al verificar la fase, con
				// GroupCoordinatorNotAvailable—. Misma espera cancelable que
				// usa startRabbit, así que tampoco retrasa la parada
				// ordenada.
				if !sleepOrDone(ctx, retryDelay) {
					return // se canceló mientras esperaba para reintentar
				}
				continue
			}
			if err := c.handle(ctx, channel, m.Value); err != nil {
				log.Printf("%s: procesando %s: %v", c.serviceName, channel, err)
			}
		}
	}()
}

// startRabbit lanza el bucle del canal y devuelve inmediatamente, igual que
// startKafka: la conexión, el canal AMQP, la declaración de la cola y el
// Consume ocurren DENTRO de la goroutine, no antes de lanzarla. Si el broker
// no está listo (o la conexión se cae más tarde), el bucle reintenta cada
// retryDelay en vez de tumbar el arranque del servicio o quedarse
// muerto — antes de esta corrección esos pasos eran síncronos dentro de
// Start(), así que un broker lento impedía arrancar el servicio entero.
func (c *Consumer) startRabbit(ctx context.Context, channel, queue string) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if ctx.Err() != nil {
				return // parada ordenada, ni lo intenta
			}

			conn, ch, deliveries, err := c.connectRabbit(queue)
			if err != nil {
				log.Printf("%s: conectando a rabbitmq para %s: %v", c.serviceName, channel, err)
				if !sleepOrDone(ctx, retryDelay) {
					return // se canceló mientras esperaba para reintentar
				}
				continue
			}

			c.consumeRabbit(ctx, channel, deliveries)
			_ = ch.Close()
			_ = conn.Close()
			if ctx.Err() != nil {
				return // parada ordenada
			}
			// Si llegamos aquí, consumeRabbit volvió porque el canal de
			// entregas se cerró (conexión perdida) y no porque se canceló el
			// contexto: vuelve a intentar la conexión desde el principio.
		}
	}()
}

// connectRabbit abre la conexión, declara la cola y arranca el Consume. Vive
// aparte de startRabbit para que el bucle de reintento pueda llamarla una y
// otra vez sin duplicar la limpieza en cada punto de fallo.
func (c *Consumer) connectRabbit(queue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	conn, err := amqpDial()
	if err != nil {
		return nil, nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("abriendo canal de rabbitmq: %w", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("declarando la cola %s: %w", queue, err)
	}
	deliveries, err := ch.Consume(queue, c.serviceName, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("consumiendo de %s: %w", queue, err)
	}
	return conn, ch, deliveries, nil
}

// consumeRabbit reenvía cada entrega al handler hasta que el contexto se
// cancela (parada ordenada) o el canal de entregas se cierra (conexión
// perdida, hay que reconectar). El llamador distingue los dos casos con
// ctx.Err() al volver.
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
				log.Printf("%s: procesando %s: %v", c.serviceName, channel, err)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

// sleepOrDone espera d, o vuelve antes si el contexto se cancela — para que
// una espera de reintento no retrase la parada ordenada del consumidor.
// Devuelve false cuando la vuelta fue por cancelación, no por haber
// terminado la espera.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// Close para los bucles y espera a que terminen. No hace falta cerrar
// conexiones de RabbitMQ aparte: cada goroutine de startRabbit cierra su
// propia conexión/canal en cuanto consumeRabbit vuelve, y vuelve de
// inmediato al cancelarse el contexto (o durante la espera de reintento, vía
// sleepOrDone) — así que esperar con wg.Wait() ya es esperar a que esa
// limpieza termine.
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
