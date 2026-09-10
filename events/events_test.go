package events

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSplitChannelSeparatesTransport(t *testing.T) {
	cases := map[string][2]string{
		"kafka:order-events": {"kafka", "order-events"},
		"rabbitmq:sms.send":  {"rabbitmq", "sms.send"},
	}
	for in, want := range cases {
		transport, name := splitChannel(in)
		if transport != want[0] || name != want[1] {
			t.Errorf("splitChannel(%q) = (%q,%q), quería (%q,%q)", in, transport, name, want[0], want[1])
		}
	}
}

func TestSplitChannelRejectsMissingTransport(t *testing.T) {
	transport, _ := splitChannel("order-events")
	if transport != "" {
		t.Errorf("un canal sin prefijo debe dar transporte vacío, dio %q", transport)
	}
}

func TestRecorderWithoutDatabaseLogsInsteadOfFailing(t *testing.T) {
	h := NewRecorder("audit-service", "")
	if err := h(context.Background(), "kafka:order-events", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("sin base de datos el recorder debe registrar y no fallar: %v", err)
	}
}

func TestRecorderRejectsUnsupportedEngine(t *testing.T) {
	h := NewRecorder("catalog-sync-service", "mongodb://mongo:27017/catalog")
	if err := h(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
		t.Fatalf("un motor no soportado debe caer al log, no fallar: %v", err)
	}
}

func TestCreateTableStatementMatchesEngine(t *testing.T) {
	pg := createTableSQL("postgres")
	my := createTableSQL("mysql")
	if !strings.Contains(pg, "SERIAL") {
		t.Errorf("postgres debe usar SERIAL: %s", pg)
	}
	if !strings.Contains(my, "AUTO_INCREMENT") {
		t.Errorf("mysql debe usar AUTO_INCREMENT: %s", my)
	}
}

func TestInsertPlaceholdersMatchEngine(t *testing.T) {
	if got := insertSQL("postgres"); !strings.Contains(got, "$1") {
		t.Errorf("postgres usa $1: %s", got)
	}
	if got := insertSQL("mysql"); !strings.Contains(got, "?") {
		t.Errorf("mysql usa ?: %s", got)
	}
}

// TestConsumerStartValidatesAllChannelsBeforeStartingAny cubre el hallazgo 3
// de la revisión de la Task 1: si un canal de la lista no trae un transporte
// reconocido, Start debe devolver el error SIN dejar goroutines colgando —
// ni siquiera las de los canales válidos que aparecen antes en la lista, que
// es justo el caso que se cuela si se valida canal a canal según se arranca
// en vez de validar todos antes de arrancar ninguno.
//
// No hace falta un broker real: kafka.NewReader no conecta hasta el primer
// ReadMessage, así que un canal "kafka:..." válido no toca la red aquí; solo
// existe para comprobar que ni siquiera SU goroutine llega a lanzarse.
func TestConsumerStartValidatesAllChannelsBeforeStartingAny(t *testing.T) {
	// runtime.GC libera goroutines que ya terminaron pero el runtime no ha
	// barrido todavía, para que la comparación de "antes" no arrastre ruido
	// de otros tests que corrieron justo antes.
	runtime.GC()
	before := runtime.NumGoroutine()

	c := NewConsumer("test-service", []string{"kafka:order-events", "sin-transporte"}, func(context.Context, string, []byte) error {
		return nil
	})

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("esperaba error por el canal sin transporte reconocido")
	}

	// Una fuga real no desaparece sola: si Start lanzó alguna goroutine antes
	// de fallar, sigue viva aquí. El margen de tiempo es solo para dejar que
	// el scheduler la registre, no para que termine por su cuenta.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before {
		t.Errorf("Start dejó %d goroutine(s) colgando (antes %d, después %d)", after-before, before, after)
	}
}
