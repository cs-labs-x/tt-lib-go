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

// El Critical de esta fase vive en NewRecorder: la memoización tiene que
// guardar SOLO el éxito de abrir la conexión y crear la tabla, nunca el fallo.
// Con un sync.Once (la primera versión del fichero) un fallo en el primer
// mensaje —el caso normal cuando compose arranca la base de datos y el
// servicio a la vez— quedaba cacheado para siempre y el consumidor no volvía a
// escribir una fila NUNCA, aunque la base de datos se recuperase segundos
// después. Es el mismo patrón que ya causó un incidente con Redis en una fase
// anterior, y hasta ahora era el único de los tres lenguajes sin test: Python
// tiene los suyos en tests/test_events.py y Node en tests/events.test.ts,
// mientras que Go se había comprobado A MANO contra un PostgreSQL real — una
// verificación que no queda en el repositorio y que nadie puede repetir.
//
// Estos tests no necesitan base de datos: registran un driver de mentira y
// sustituyen `openDB` (ver recorder.go) por uno que lo devuelve, así que
// corren en `go test ./...` sin infraestructura.

// --- Driver de mentira -----------------------------------------------------

// fakeDriver cuenta las consultas y decide cuáles fallan. Implementa lo
// mínimo de database/sql/driver para que ExecContext funcione: el paquete sql
// usa ExecerContext directamente si el Conn lo implementa, así que no hace
// falta preparar sentencias.
type fakeDriver struct {
	mu sync.Mutex
	// createFailures es cuántos CREATE TABLE deben fallar antes de empezar a
	// funcionar. Modela una base de datos que todavía no acepta conexiones.
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

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no usado") }

func (c *fakeConn) Close() error {
	c.drv.mu.Lock()
	defer c.drv.mu.Unlock()
	c.drv.closed++
	return nil
}

func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("no usado") }

func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.drv.mu.Lock()
	defer c.drv.mu.Unlock()
	if strings.Contains(query, "CREATE TABLE") {
		c.drv.creates++
		if c.drv.creates <= c.drv.createFailures {
			return nil, errors.New("la base de datos todavía no acepta conexiones")
		}
		return driver.RowsAffected(0), nil
	}
	c.drv.inserts++
	return driver.RowsAffected(1), nil
}

// Sin ResetSession/IsValid database/sql reutiliza la conexión del pool tal
// cual, que es lo que queremos: así los recuentos son deterministas.
var _ driver.ExecerContext = (*fakeConn)(nil)

// withFakeDB sustituye openDB por uno que devuelve un *sql.DB sobre drv, y lo
// restaura al terminar el test. Cada test registra su driver con un nombre
// único: database/sql entra en pánico si se registra dos veces el mismo.
func withFakeDB(t *testing.T, drv *fakeDriver) {
	t.Helper()
	name := "fake-" + t.Name()
	sql.Register(name, drv)
	previous := openDB
	openDB = func(_, _ string) (*sql.DB, error) { return sql.Open(name, "") }
	t.Cleanup(func() { openDB = previous })
}

// --- Los tests -------------------------------------------------------------

// El test que fija el Critical.
func TestNewRecorderNoCacheaElFallo(t *testing.T) {
	drv := &fakeDriver{createFailures: 1}
	withFakeDB(t, drv)

	record := NewRecorder("audit-service", "postgres://tt:tt@postgres:5432/audit")
	ctx := context.Background()

	// Primer mensaje: la base de datos aún no responde y el CREATE TABLE falla.
	if err := record(ctx, "kafka:order-events", []byte(`{"a":1}`)); err == nil {
		t.Fatal("se esperaba error en el primer mensaje, la base de datos no respondía")
	}

	// Segundo mensaje: la base de datos ya responde. Si el fallo se hubiera
	// cacheado, este mensaje devolvería el mismo error sin volver a intentarlo
	// y el consumidor se quedaría mudo para siempre.
	if err := record(ctx, "kafka:order-events", []byte(`{"a":2}`)); err != nil {
		t.Fatalf("el segundo mensaje debía reintentar y funcionar, pero falló: %v", err)
	}

	creates, inserts, _, closed := drv.counts()
	if creates != 2 {
		t.Errorf("el CREATE TABLE debía reintentarse: %d intentos, se esperaban 2", creates)
	}
	if inserts != 1 {
		t.Errorf("filas insertadas: %d, se esperaba 1", inserts)
	}
	// La conexión del intento fallido se cierra, no se abandona.
	if closed == 0 {
		t.Error("la conexión del intento fallido debía cerrarse")
	}
}

// La otra mitad del contrato: el ÉXITO sí se memoiza. Sin esto, "no cachear el
// fallo" se podría satisfacer abriendo una conexión nueva en cada mensaje.
func TestNewRecorderMemoizaElExito(t *testing.T) {
	drv := &fakeDriver{}
	withFakeDB(t, drv)

	record := NewRecorder("audit-service", "postgres://tt:tt@postgres:5432/audit")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := record(ctx, "kafka:order-events", []byte(`{}`)); err != nil {
			t.Fatalf("mensaje %d: %v", i, err)
		}
	}

	creates, inserts, opens, _ := drv.counts()
	if creates != 1 {
		t.Errorf("el CREATE TABLE debía correr una sola vez, corrió %d", creates)
	}
	if opens != 1 {
		t.Errorf("debía abrirse una sola conexión, se abrieron %d", opens)
	}
	if inserts != 5 {
		t.Errorf("filas insertadas: %d, se esperaban 5", inserts)
	}
}

// MySQL pasa por la conversión de URL a DSN y por los placeholders `?`. Se
// comprueba que ese camino también llega a insertar.
func TestNewRecorderMysqlUsaSusPlaceholders(t *testing.T) {
	drv := &fakeDriver{}
	withFakeDB(t, drv)

	record := NewRecorder("seat-service", "mysql://tt:tt@mysql:3306/seat")
	if err := record(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
		t.Fatalf("no debía fallar: %v", err)
	}
	if _, inserts, _, _ := drv.counts(); inserts != 1 {
		t.Errorf("filas insertadas: %d, se esperaba 1", inserts)
	}
	if got := insertSQL("mysql"); !strings.Contains(got, "(?, ?)") {
		t.Errorf("MySQL usa `?`, no `$1`: %q", got)
	}
	if got := createTableSQL("mysql"); !strings.Contains(got, "AUTO_INCREMENT") {
		t.Errorf("MySQL usa AUTO_INCREMENT, no SERIAL: %q", got)
	}
}

// Sin base de datos, o con una que no es SQL soportado (MongoDB, hoy
// catalog-sync-service), el manejador registra en el log y NO falla: un
// consumidor sin base de datos no debe nackear todos sus mensajes.
func TestNewRecorderSinSQLRegistraEnLog(t *testing.T) {
	for _, url := range []string{"", "mongodb://tt:tt@mongo:27017/catalog"} {
		record := NewRecorder("catalog-sync-service", url)
		if err := record(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
			t.Errorf("con DATABASE_URL %q no debía fallar: %v", url, err)
		}
	}
}

// Una DATABASE_URL de MySQL que no se puede parsear no puede arreglarse
// reintentando, pero tampoco debe tumbar el bucle de consumo: devuelve error
// en cada mensaje y ya.
func TestNewRecorderUrlMysqlInvalida(t *testing.T) {
	record := NewRecorder("seat-service", "mysql://tt:tt@mysql:3306/seat\x7f")
	err := record(context.Background(), "kafka:travel-events", []byte(`{}`))
	if err == nil {
		t.Fatal("se esperaba error de DATABASE_URL inválida")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL inválida") {
		t.Errorf("error inesperado: %v", err)
	}
}
