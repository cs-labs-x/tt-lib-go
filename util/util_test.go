package util

import (
	"testing"
	"time"
)

func TestMoney(t *testing.T) {
	for in, want := range map[string]int64{"12.34": 1234, "7": 700, "-0.5": -50} {
		got, err := ParseMoney(in)
		if err != nil || got != want {
			t.Fatalf("ParseMoney(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ParseMoney("12,34"); err == nil {
		t.Fatal("expected an error for a comma decimal")
	}
	if got := FormatMoney(1234, ""); got != "12.34 EUR" {
		t.Fatalf("FormatMoney = %q", got)
	}
	if got := FormatMoney(-5, "GBP"); got != "-0.05 GBP" {
		t.Fatalf("FormatMoney = %q", got)
	}
}

func TestStationHashHours(t *testing.T) {
	if got := NormalizeStationCode(" mad-01 "); got != "MAD01" {
		t.Fatalf("NormalizeStationCode = %q", got)
	}
	if StableHash("a", "b") != StableHash("a", "b") || StableHash("a", "b") == StableHash("ab") || len(StableHash("x")) != 16 {
		t.Fatal("StableHash is not stable, or not distinct, or not 16 chars")
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := HoursUntil(now.Add(6*time.Hour), now); got != 6 {
		t.Fatalf("HoursUntil = %v", got)
	}
}

func TestBackoffRedactNormalize(t *testing.T) {
	want := []int{200, 400, 800, 1600}
	for i, w := range want {
		if got := BackoffDelayMs(i); got != w {
			t.Fatalf("BackoffDelayMs(%d) = %d", i, got)
		}
	}
	if BackoffDelayMs(20) != 30_000 {
		t.Fatal("expected the ceiling")
	}
	if got := RedactPII("mail ana@example.com or +34 600 123 456"); got != "mail [email] or [phone]" {
		t.Fatalf("RedactPII = %q", got)
	}
	if got := Normalize("  a   b \n c "); got != "a b c" {
		t.Fatalf("Normalize = %q", got)
	}
}
