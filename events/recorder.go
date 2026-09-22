package events

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// createTableSQL devuelve la sentencia de creación para el motor indicado.
// Los tipos autoincrementales no son portables, así que hay una por motor.
func createTableSQL(engine string) string {
	id := "SERIAL PRIMARY KEY"
	if engine == "mysql" {
		id = "BIGINT AUTO_INCREMENT PRIMARY KEY"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS received_events (
		id %s,
		channel VARCHAR(255) NOT NULL,
		payload TEXT NOT NULL,
		received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`, id)
}

// insertSQL devuelve el INSERT con los placeholders del motor: PostgreSQL
// usa $1 y MySQL usa ?.
func insertSQL(engine string) string {
	if engine == "mysql" {
		return "INSERT INTO received_events (channel, payload) VALUES (?, ?)"
	}
	return "INSERT INTO received_events (channel, payload) VALUES ($1, $2)"
}

// openDB abre la conexión. Es una variable, y no una llamada directa a
// sql.Open, para que recorder_test.go pueda sustituirla por un driver de
// mentira: el hallazgo grave de esta fase —memoizar el ÉXITO y NO el fallo—
// se había verificado a mano contra un PostgreSQL real, y esa verificación no
// quedaba en el repositorio. Es el único punto del fichero que toca el mundo
// exterior, así que es el único que hay que poder sustituir; el resto de la
// lógica (cuándo se reintenta, cuándo se cachea) queda cubierta tal cual está
// escrita. Nada de producción la reasigna.
var openDB = sql.Open

// NewRecorder devuelve el manejador que registra lo recibido. Si el servicio
// tiene una base de datos SQL, escribe una fila; si no la tiene, o si el
// motor no es SQL — hoy solo catalog-sync-service, con MongoDB —, registra
// en el log. Es una decisión, no un olvido: cubrir los tres motores en los
// tres lenguajes serían nueve caminos de código para nueve consumidores.
func NewRecorder(serviceName, databaseURL string) Handler {
	engine, driver, dsn, dsnErr := engineOf(databaseURL)
	if engine == "" {
		return func(_ context.Context, channel string, payload []byte) error {
			log.Printf("%s: recibido de %s: %s", serviceName, channel, payload)
			return nil
		}
	}

	// Solo se memoiza el ÉXITO de abrir la conexión y crear la tabla, nunca
	// el fallo. Con un sync.Once (la primera versión de este fichero) un
	// fallo en el primer mensaje —el caso normal cuando compose arranca la
	// base de datos y el servicio a la vez— quedaba cacheado para siempre:
	// ningún mensaje posterior volvía a intentarlo, aunque la base de datos
	// se recuperase segundos después. Es el mismo patrón que ya mordió a
	// seat-service en la fase 1 con un sync.Once sobre la conexión a Redis.
	// Por eso `db` solo se lee/escribe bajo `mu`, y solo se guarda cuando la
	// conexión Y el CREATE TABLE salieron bien; si algo falla, `db` se queda
	// en nil y el siguiente mensaje vuelve a intentarlo desde cero.
	var (
		mu sync.Mutex
		db *sql.DB
	)
	ensure := func(ctx context.Context) (*sql.DB, error) {
		mu.Lock()
		defer mu.Unlock()
		if db != nil {
			return db, nil
		}
		if dsnErr != nil {
			// Error de parseo de una URL fija: nunca se va a arreglar solo
			// reintentando, pero tampoco tiene sentido cachearlo aparte —
			// es barato de recalcular y así todo el estado vive en `db`.
			return nil, fmt.Errorf("%s: DATABASE_URL inválida: %w", serviceName, dsnErr)
		}
		conn, err := openDB(driver, dsn)
		if err != nil {
			return nil, fmt.Errorf("%s: abriendo conexión: %w", serviceName, err)
		}
		if _, err := conn.ExecContext(ctx, createTableSQL(engine)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s: preparando received_events: %w", serviceName, err)
		}
		db = conn
		return db, nil
	}
	return func(ctx context.Context, channel string, payload []byte) error {
		conn, err := ensure(ctx)
		if err != nil {
			return err
		}
		_, execErr := conn.ExecContext(ctx, insertSQL(engine), channel, string(payload))
		return execErr
	}
}

// engineOf deduce el motor, su driver y el DSN que ese driver espera a
// partir de la cadena de conexión. Devuelve motor vacío para cualquier cosa
// que no sea SQL soportado (hoy: MongoDB, el único caso entre los
// consumidores Go).
func engineOf(databaseURL string) (engine, driver, dsn string, err error) {
	switch {
	case strings.HasPrefix(databaseURL, "postgresql://"), strings.HasPrefix(databaseURL, "postgres://"):
		// El driver lib/pq acepta la URL tal cual.
		return "postgres", "postgres", databaseURL, nil
	case strings.HasPrefix(databaseURL, "mysql://"):
		dsn, err := mysqlDSN(databaseURL)
		return "mysql", "mysql", dsn, err
	default:
		return "", "", "", nil
	}
}

// mysqlDSN convierte la URL que inyecta el compose
// (`mysql://tt:tt@mysql:3306/seat`) al formato DSN que espera
// go-sql-driver/mysql (`tt:tt@tcp(mysql:3306)/seat`) — el driver no acepta
// el esquema `mysql://` directamente. Misma conversión que
// services/tt-seat-service/internal/booking/check_availability.go
// (mysqlDSN); replicada en vez de importada porque ese paquete es código de
// negocio escrito a mano de un servicio concreto, no una librería
// compartida — tt-lib-go no debe depender de un servicio.
func mysqlDSN(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	pass, _ := u.User.Password()
	name := strings.TrimPrefix(u.Path, "/")
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", u.User.Username(), pass, u.Host, name), nil
}
