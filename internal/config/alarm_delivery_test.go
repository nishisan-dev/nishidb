package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWebhookDefaultsAndOverrides(t *testing.T) {
	cfg, err := Load(writeConfig(t, "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: http://localhost/events\n"))
	if err != nil {
		t.Fatal(err)
	}
	timeout, bytes, err := cfg.AlarmDelivery.Webhooks[0].Limits()
	if err != nil || timeout != 5*time.Second || bytes != 256<<20 {
		t.Fatalf("defaults incorretos: %s %d %v", timeout, bytes, err)
	}
	cfg, err = Load(writeConfig(t, "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: https://example.com/events\n      timeout: 9s\n      queue:\n        maxBytes: 64MiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	timeout, bytes, err = cfg.AlarmDelivery.Webhooks[0].Limits()
	if err != nil || timeout != 9*time.Second || bytes != 64<<20 {
		t.Fatalf("overrides não chegaram: %s %d %v", timeout, bytes, err)
	}
}

func TestWebhookConfigurationRefusesInvalidValuesWithOwnDiagnostic(t *testing.T) {
	base := "alarmDelivery:\n  webhooks:\n    - name: noc\n      url: https://example.com/events\n"
	cases := []struct{ body, message string }{
		{base + "      timout: 2s\n", "timout"},
		{base + "      timeout: 0s\n", "timeout must"},
		{base + "      timeout: \"\"\n", "timeout must not be blank"},
		{base + "      timeout: 6m\n", "timeout must"},
		{base + "      queue:\n        maxBytes: 1MiB\n", "queue.maxBytes must"},
		{base + "      queue:\n        maxByte: 4MiB\n", "maxByte"},
		{strings.Replace(base, "name: noc", "name: ../noc", 1), "invalid or duplicate webhook name"},
		{strings.Replace(base, "https://example.com/events", "ftp://example.com/events", 1), "requires an HTTP(S) URL"},
		{strings.Replace(base, "https://example.com/events", "https://secret:secret@example.com/events", 1), "requires an HTTP(S) URL"},
		{base + "    - name: noc\n      url: http://example.com\n", "duplicate webhook name"},
		{"alarmDelivery:\n  webhoks: []\n", "webhoks"},
	}
	for _, tc := range cases {
		_, err := Load(writeConfig(t, tc.body))
		if !errors.Is(err, ErrAlarmDelivery) || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("recusa não veio da guarda esperada %q: %v", tc.message, err)
		}
	}
}
