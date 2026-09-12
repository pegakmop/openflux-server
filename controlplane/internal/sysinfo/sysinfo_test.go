package sysinfo

import (
	"testing"
)

func TestParseFloatLine(t *testing.T) {
	cases := []struct {
		name   string
		data   string
		prefix string
		max    int
		want   []float64
	}{
		{"loadavg", "0.51 0.42 0.39 1/123 456\n", "", 3, []float64{0.51, 0.42, 0.39}},
		{"uptime", "12345.67 78901.23\n", "", 2, []float64{12345.67, 78901.23}},
		{"no match", "foo 1 2\n", "cpu ", 2, nil},
		{"wrong count", "1 2 3 4\n", "", 2, []float64{1, 2}},
		{"bad number", "1 x 3\n", "", 3, nil},
		{"empty fields", "", "", 3, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseFloatLine(c.data, c.prefix, c.max)
			if len(got) != len(c.want) {
				t.Fatalf("parseFloatLine(%q) = %v, want %v", c.data, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("field %d = %v, want %v", i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestCalcMem(t *testing.T) {
	sample := "MemTotal:       16384000 kB\n" +
		"MemFree:         1024000 kB\n" +
		"MemAvailable:   8192000 kB\n" +
		"SwapTotal:       2097152 kB\n" +
		"SwapFree:         524288 kB\n"
	total, used, swapTotal, swapUsed := calcMem([]byte(sample))

	if want := uint64(16384000 * 1024); total != want {
		t.Errorf("total = %d, want %d", total, want)
	}
	if want := uint64((16384000 - 8192000) * 1024); used != want {
		t.Errorf("used = %d, want %d", used, want)
	}
	if want := uint64(2097152 * 1024); swapTotal != want {
		t.Errorf("swapTotal = %d, want %d", swapTotal, want)
	}
	if want := uint64((2097152 - 524288) * 1024); swapUsed != want {
		t.Errorf("swapUsed = %d, want %d", swapUsed, want)
	}
}

func TestCalcMemEmpty(t *testing.T) {
	total, used, swapTotal, swapUsed := calcMem(nil)
	if total != 0 || used != 0 || swapTotal != 0 || swapUsed != 0 {
		t.Errorf("empty input should yield all zeros, got %d %d %d %d", total, used, swapTotal, swapUsed)
	}
}
