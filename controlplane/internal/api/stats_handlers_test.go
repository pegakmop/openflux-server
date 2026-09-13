package api

import "testing"

func TestClampDays(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 30},
		{"7", 7},
		{"30", 30},
		{"365", 365},
		{"366", 365},
		{"0", 30},
		{"-5", 30},
		{"abc", 30},
	}
	for _, c := range cases {
		if got := clampDays(c.in); got != c.want {
			t.Errorf("clampDays(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
