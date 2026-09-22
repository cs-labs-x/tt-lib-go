package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

// The Critical of this phase lives in NewRecorder: the memoization has to
// keep ONLY the success of opening the connection and creating the table,
// never the failure. With a sync.Once (the first version of the file) a
// failure on the first message — the normal case when compose starts the
// database and the service at the same time — stayed cached forever and the
// consumer NEVER wrote a row again, even if the database recovered seconds
// later. It is the same pattern that already caused an incident with Redis in
// an earlier phase, and until now it was the only one of the three languages
// with no test: Python has its own in tests/test_events.py and Node in
// tests/events.test.ts, while Go had been checked BY HAND against a real
// PostgreSQL — a verification that is not left in the repository and that
// nobody can repeat.
//
// These tests need no database: they register a fake driver and swap `openDB`
// (see recorder.go) for one that returns it, so they run under
// `go test ./...` with no infrastructure.

// --- Fake driver -----------------------------------------------------------

// fakeDriver counts the queries and decides which ones fail. It implements
// the minimum of database/sql/driver for ExecContext to work: the sql package
// uses ExecerContext directly if the Conn implements it, so there is no need
// to prepare statements.
type fakeDriver struct {
	mu sync.Mutex
	// createFailures is how many CREATE TABLE statements must fail before it
	// starts working. It models a database that does not accept connections
	// yet.
	createFailures int
	creates        int
	inserts        int
	opens          int
	closed         int
}

func (d *fakeDriver) counts() (creates, inserts, opens, closed int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.creates, d.inserts, d.opens, d.closed
}

func (d *fakeDriver) Open(string) (driver.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opens++
	return &fakeConn{drv: d}, nil
}

type fakeConn struct{ drv *fakeDriver }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not used") }

func (c *fakeConn) Close() error {
	c.drv.mu.Lock()
	defer c.drv.mu.Unlock()
	c.drv.closed++
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("not used") }

func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.drv.mu.Lock()
	defer c.drv.mu.Unlock()
	if strings.Contains(query, "CREATE TABLE") {
		c.drv.creates++
		if c.drv.creates <= c.drv.createFailures {
			return nil, errors.New("the database does not accept connections yet")
		}
		return driver.RowsAffected(0), nil
	}
	c.drv.inserts++
	return driver.RowsAffected(1), nil
}

// Without ResetSession/IsValid, database/sql reuses the pooled connection as
// it is, which is what we want: that way the counts are deterministic.
var _ driver.ExecerContext = (*fakeConn)(nil)

// withFakeDB swaps openDB for one that returns a *sql.DB over drv, and puts
// it back when the test ends. Each test registers its driver under a unique
// name: database/sql panics if the same one is registered twice.
func withFakeDB(t *testing.T, drv *fakeDriver) {
	t.Helper()
	name := "fake-" + t.Name()
	sql.Register(name, drv)
	previous := openDB
	openDB = func(_, _ string) (*sql.DB, error) { return sql.Open(name, "") }
	t.Cleanup(func() { openDB = previous })
}

// --- The tests -------------------------------------------------------------

// The test that pins the Critical.
func TestNewRecorderDoesNotCacheFailure(t *testing.T) {
	drv := &fakeDriver{createFailures: 1}
	withFakeDB(t, drv)

	record := NewRecorder("audit-service", "postgres://tt:tt@postgres:5432/audit")
	ctx := context.Background()

	// First message: the database does not answer yet and the CREATE TABLE fails.
	if err := record(ctx, "kafka:order-events", []byte(`{"a":1}`)); err == nil {
		t.Fatal("wanted an error on the first message, the database was not answering")
	}

	// Second message: the database answers now. If the failure had been
	// cached, this message would return the same error without trying again
	// and the consumer would stay mute forever.
	if err := record(ctx, "kafka:order-events", []byte(`{"a":2}`)); err != nil {
		t.Fatalf("the second message had to retry and work, but it failed: %v", err)
	}

	creates, inserts, _, closed := drv.counts()
	if creates != 2 {
		t.Errorf("the CREATE TABLE had to be retried: %d attempts, wanted 2", creates)
	}
	if inserts != 1 {
		t.Errorf("rows inserted: %d, wanted 1", inserts)
	}
	// The connection of the failed attempt is closed, not abandoned.
	if closed == 0 {
		t.Error("the connection of the failed attempt had to be closed")
	}
}

// The other half of the contract: SUCCESS is memoized. Without this, "do not
// cache the failure" could be met by opening a new connection on every
// message.
func TestNewRecorderMemoizesSuccess(t *testing.T) {
	drv := &fakeDriver{}
	withFakeDB(t, drv)

	record := NewRecorder("audit-service", "postgres://tt:tt@postgres:5432/audit")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := record(ctx, "kafka:order-events", []byte(`{}`)); err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
	}

	creates, inserts, opens, _ := drv.counts()
	if creates != 1 {
		t.Errorf("the CREATE TABLE had to run only once, it ran %d", creates)
	}
	if opens != 1 {
		t.Errorf("only one connection had to be opened, %d were opened", opens)
	}
	if inserts != 5 {
		t.Errorf("rows inserted: %d, wanted 5", inserts)
	}
}

// MySQL goes through the URL-to-DSN conversion and through the `?`
// placeholders. This checks that path also gets as far as inserting.
func TestNewRecorderMysqlUsesItsPlaceholders(t *testing.T) {
	drv := &fakeDriver{}
	withFakeDB(t, drv)

	record := NewRecorder("seat-service", "mysql://tt:tt@mysql:3306/seat")
	if err := record(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
		t.Fatalf("it must not fail: %v", err)
	}
	if _, inserts, _, _ := drv.counts(); inserts != 1 {
		t.Errorf("rows inserted: %d, wanted 1", inserts)
	}
	if got := insertSQL("mysql"); !strings.Contains(got, "(?, ?)") {
		t.Errorf("MySQL uses `?`, not `$1`: %q", got)
	}
	if got := createTableSQL("mysql"); !strings.Contains(got, "AUTO_INCREMENT") {
		t.Errorf("MySQL uses AUTO_INCREMENT, not SERIAL: %q", got)
	}
}

// With no database, or with one that is not supported SQL (MongoDB, today
// catalog-sync-service), the handler writes to the log and does NOT fail: a
// consumer with no database must not nack all of its messages.
func TestNewRecorderWithoutSQLOnlyLogs(t *testing.T) {
	for _, url := range []string{"", "mongodb://tt:tt@mongo:27017/catalog"} {
		record := NewRecorder("catalog-sync-service", url)
		if err := record(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
			t.Errorf("with DATABASE_URL %q it must not fail: %v", url, err)
		}
	}
}

// A MySQL DATABASE_URL that cannot be parsed cannot be fixed by retrying,
// but it must not take down the consume loop either: it returns an error on
// every message and that is all.
func TestNewRecorderInvalidMysqlURL(t *testing.T) {
	record := NewRecorder("seat-service", "mysql://tt:tt@mysql:3306/seat\x7f")
	err := record(context.Background(), "kafka:travel-events", []byte(`{}`))
	if err == nil {
		t.Fatal("wanted an invalid DATABASE_URL error")
	}
	if !strings.Contains(err.Error(), "invalid DATABASE_URL") {
		t.Errorf("unexpected error: %v", err)
	}
}
