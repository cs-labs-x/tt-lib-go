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

// createTableSQL returns the create statement for the given engine.
// Auto-increment types are not portable, so there is one per engine.
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

// insertSQL returns the INSERT with the placeholders of the engine:
// PostgreSQL uses $1 and MySQL uses ?.
func insertSQL(engine string) string {
	if engine == "mysql" {
		return "INSERT INTO received_events (channel, payload) VALUES (?, ?)"
	}
	return "INSERT INTO received_events (channel, payload) VALUES ($1, $2)"
}

// openDB opens the connection. It is a variable, and not a direct call to
// sql.Open, so that recorder_test.go can swap it for a fake driver: the
// serious finding of this phase — memoizing SUCCESS and NOT the failure — had
// been verified by hand against a real PostgreSQL, and that verification was
// not left in the repository. It is the only point of the file that touches
// the outside world, so it is the only one that has to be swappable; the rest
// of the logic (when it retries, when it caches) is covered as written.
// Nothing in production reassigns it.
var openDB = sql.Open

// NewRecorder returns the handler that records what comes in. If the service
// has a SQL database, it writes a row; if it has none, or if the engine is
// not SQL — today only catalog-sync-service, with MongoDB —, it writes to the
// log. It is a decision, not an oversight: covering the three engines in the
// three languages would be nine code paths for nine consumers.
func NewRecorder(serviceName, databaseURL string) Handler {
	engine, driver, dsn, dsnErr := engineOf(databaseURL)
	if engine == "" {
		return func(_ context.Context, channel string, payload []byte) error {
			log.Printf("%s: received from %s: %s", serviceName, channel, payload)
			return nil
		}
	}

	// Only the SUCCESS of opening the connection and creating the table is
	// memoized, never the failure. With a sync.Once (the first version of
	// this file) a failure on the first message — the normal case when
	// compose starts the database and the service at the same time — stayed
	// cached forever: no later message tried again, even if the database
	// recovered seconds later. It is the same pattern that already bit
	// seat-service in phase 1 with a sync.Once over the Redis connection.
	// That is why `db` is only read/written under `mu`, and is only stored
	// when the connection AND the CREATE TABLE went well; if anything fails,
	// `db` stays nil and the next message tries again from scratch.
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
			// Parse error of a fixed URL: retrying will never fix it on its
			// own, but caching it apart makes no sense either — it is cheap
			// to recompute and this way all the state lives in `db`.
			return nil, fmt.Errorf("%s: invalid DATABASE_URL: %w", serviceName, dsnErr)
		}
		conn, err := openDB(driver, dsn)
		if err != nil {
			return nil, fmt.Errorf("%s: opening connection: %w", serviceName, err)
		}
		if _, err := conn.ExecContext(ctx, createTableSQL(engine)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s: preparing received_events: %w", serviceName, err)
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

// engineOf works out the engine, its driver and the DSN that driver expects
// from the connection string. It returns an empty engine for anything that
// is not supported SQL (today: MongoDB, the only case among the Go
// consumers).
func engineOf(databaseURL string) (engine, driver, dsn string, err error) {
	switch {
	case strings.HasPrefix(databaseURL, "postgresql://"), strings.HasPrefix(databaseURL, "postgres://"):
		// The lib/pq driver takes the URL as it is.
		return "postgres", "postgres", databaseURL, nil
	case strings.HasPrefix(databaseURL, "mysql://"):
		dsn, err := mysqlDSN(databaseURL)
		return "mysql", "mysql", dsn, err
	default:
		return "", "", "", nil
	}
}

// mysqlDSN converts the URL the compose injects
// (`mysql://tt:tt@mysql:3306/seat`) into the DSN format
// go-sql-driver/mysql expects (`tt:tt@tcp(mysql:3306)/seat`) — the driver
// does not take the `mysql://` scheme directly. Same conversion as
// services/tt-seat-service/internal/booking/check_availability.go
// (mysqlDSN); replicated instead of imported because that package is
// hand-written business code of one concrete service, not a shared
// library — tt-lib-go must not depend on a service.
func mysqlDSN(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	pass, _ := u.User.Password()
	name := strings.TrimPrefix(u.Path, "/")
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", u.User.Username(), pass, u.Host, name), nil
}
