// Package util holds the small helpers every service reaches for. They live
// here so that a rule ("a station code is three upper-case letters") is
// decided once, not once per service.
package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var moneyPattern = regexp.MustCompile(`^\s*(-?)(\d+)(?:\.(\d{1,2}))?\s*$`)

// ParseMoney turns "12.34" into 1234 minor units. Anything that is not a plain
// decimal is an error.
func ParseMoney(text string) (int64, error) {
	m := moneyPattern.FindStringSubmatch(text)
	if m == nil {
		return 0, fmt.Errorf("not a money amount: %q", text)
	}
	whole, _ := strconv.ParseInt(m[2], 10, 64)
	frac := m[3]
	for len(frac) < 2 {
		frac += "0"
	}
	cents, _ := strconv.ParseInt(frac, 10, 64)
	minor := whole*100 + cents
	if m[1] == "-" {
		minor = -minor
	}
	return minor, nil
}

// FormatMoney turns 1234, "EUR" into "12.34 EUR".
func FormatMoney(minor int64, currency string) string {
	if currency == "" {
		currency = "EUR"
	}
	sign := ""
	if minor < 0 {
		sign = "-"
		minor = -minor
	}
	return fmt.Sprintf("%s%d.%02d %s", sign, minor/100, minor%100, currency)
}

var notStationChar = regexp.MustCompile(`[^A-Z0-9]`)

// NormalizeStationCode turns "  mad-01 " into "MAD01": upper-case alphanumerics, nothing else.
func NormalizeStationCode(code string) string {
	return notStationChar.ReplaceAllString(strings.ToUpper(code), "")
}

// StableHash is a short, stable digest of the given parts, for keys and fingerprints.
func StableHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])[:16]
}

// HoursUntil is the number of hours from now until at; negative when at is in the past.
func HoursUntil(at, now time.Time) float64 {
	return at.Sub(now).Hours()
}

// BackoffDelayMs is exponential backoff with a 30 s ceiling: 200, 400, 800 … ms.
func BackoffDelayMs(attempt int) int {
	if attempt < 0 {
		attempt = 0
	}
	d := 200 << uint(attempt)
	if d > 30_000 || d <= 0 {
		return 30_000
	}
	return d
}

var (
	emailPattern = regexp.MustCompile(`[^\s@]+@[^\s@]+\.[^\s@]+`)
	phonePattern = regexp.MustCompile(`\+?\d[\d\s-]{6,}\d`)
)

// RedactPII masks e-mail addresses and phone numbers in free text before it is logged.
func RedactPII(text string) string {
	return phonePattern.ReplaceAllString(emailPattern.ReplaceAllString(text, "[email]"), "[phone]")
}

var spaces = regexp.MustCompile(`\s+`)

// Normalize trims and collapses whitespace. Several services keep a local
// normalize of their own; this is the shared one.
func Normalize(text string) string {
	return spaces.ReplaceAllString(strings.TrimSpace(text), " ")
}
