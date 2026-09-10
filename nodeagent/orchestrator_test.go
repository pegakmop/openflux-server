package nodeagent

import "testing"

func TestDiffCounter(t *testing.T) {
	cases := []struct {
		name              string
		previous, current uint64
		want              uint64
	}{
		{"normal growth", 100, 150, 50},
		{"no change", 100, 100, 0},
		{"reconnect reset treated as count-from-zero", 500, 20, 20},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := diffCounter(c.previous, c.current); got != c.want {
				t.Errorf("diffCounter(%d, %d) = %d, want %d", c.previous, c.current, got, c.want)
			}
		})
	}
}
