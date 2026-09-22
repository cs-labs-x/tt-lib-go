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
			t.Errorf("splitChannel(%q) = (%q,%q), wanted (%q,%q)", in, transport, name, want[0], want[1])
		}
	}
}

func TestSplitChannelRejectsMissingTransport(t *testing.T) {
	transport, _ := splitChannel("order-events")
	if transport != "" {
		t.Errorf("a channel with no prefix must give an empty transport, gave %q", transport)
	}
}

func TestRecorderWithoutDatabaseLogsInsteadOfFailing(t *testing.T) {
	h := NewRecorder("audit-service", "")
	if err := h(context.Background(), "kafka:order-events", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("with no database the recorder must log and not fail: %v", err)
	}
}

func TestRecorderRejectsUnsupportedEngine(t *testing.T) {
	h := NewRecorder("catalog-sync-service", "mongodb://mongo:27017/catalog")
	if err := h(context.Background(), "kafka:travel-events", []byte(`{}`)); err != nil {
		t.Fatalf("an unsupported engine must fall back to the log, not fail: %v", err)
	}
}

func TestCreateTableStatementMatchesEngine(t *testing.T) {
	pg := createTableSQL("postgres")
	my := createTableSQL("mysql")
	if !strings.Contains(pg, "SERIAL") {
		t.Errorf("postgres must use SERIAL: %s", pg)
	}
	if !strings.Contains(my, "AUTO_INCREMENT") {
		t.Errorf("mysql must use AUTO_INCREMENT: %s", my)
	}
}

func TestInsertPlaceholdersMatchEngine(t *testing.T) {
	if got := insertSQL("postgres"); !strings.Contains(got, "$1") {
		t.Errorf("postgres uses $1: %s", got)
	}
	if got := insertSQL("mysql"); !strings.Contains(got, "?") {
		t.Errorf("mysql uses ?: %s", got)
	}
}

// TestConsumerStartValidatesAllChannelsBeforeStartingAny covers finding 3 of
// the Task 1 review: if one channel of the list does not carry a recognized
// transport, Start must return the error WITHOUT leaving goroutines hanging —
// not even those of the valid channels that come earlier in the list, which
// is exactly the case that slips through when the check is done channel by
// channel as they start instead of checking them all before starting any.
//
// No real broker is needed: kafka.NewReader does not connect until the first
// ReadMessage, so a valid "kafka:..." channel does not touch the network
// here; it only exists to check that not even ITS goroutine gets launched.
func TestConsumerStartValidatesAllChannelsBeforeStartingAny(t *testing.T) {
	// runtime.GC frees goroutines that already finished but the runtime has
	// not swept yet, so the "before" comparison does not carry noise from
	// other tests that ran just before.
	runtime.GC()
	before := runtime.NumGoroutine()

	c := NewConsumer("test-service", []string{"kafka:order-events", "no-transport"}, func(context.Context, string, []byte) error {
		return nil
	})

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("wanted an error for the channel with no recognized transport")
	}

	// A real leak does not go away on its own: if Start launched any
	// goroutine before failing, it is still alive here. The time margin is
	// only to let the scheduler register it, not for it to finish by itself.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before {
		t.Errorf("Start left %d goroutine(s) hanging (before %d, after %d)", after-before, before, after)
	}
}
