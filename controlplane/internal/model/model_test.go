package model

import (
	"testing"
	"time"
)

func TestKeyStatus(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	limit := int64(1000)

	cases := []struct {
		name string
		key  Key
		want string
	}{
		{
			name: "disabled wins over everything else",
			key:  Key{Enabled: false, TrafficLimitBytes: &limit, BytesSentTotal: 2000},
			want: "disabled",
		},
		{
			name: "expired counts as disabled",
			key:  Key{Enabled: true, ExpiresAt: ptrTime(now.Add(-time.Hour))},
			want: "disabled",
		},
		{
			name: "over quota when usage reaches the limit",
			key:  Key{Enabled: true, TrafficLimitBytes: &limit, BytesSentTotal: 600, BytesReceivedTotal: 400},
			want: "over_quota",
		},
		{
			name: "active under quota with no expiry",
			key:  Key{Enabled: true, TrafficLimitBytes: &limit, BytesSentTotal: 100, BytesReceivedTotal: 100},
			want: "active",
		},
		{
			name: "active with unlimited quota",
			key:  Key{Enabled: true, BytesSentTotal: 999999},
			want: "active",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.key.Status(now); got != c.want {
				t.Errorf("Status() = %q, want %q", got, c.want)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
