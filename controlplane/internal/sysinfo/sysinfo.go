// Package sysinfo reads basic host metrics straight from Linux /proc and
// syscall-level statfs - deliberately dependency-free, because the only
// other Go dependency in this module is the Postgres driver and adding
// gopsutil for three numbers isn't worth it. Everything here is Linux-only
// (the controlplane is deployed on Debian/Ubuntu VPSes); on any other OS
// the read functions return zero values instead of erroring out, so the
// panel just shows "—" rather than failing.
package sysinfo

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Sample is one point-in-time snapshot of the host.
type Sample struct {
	Hostname   string  `json:"hostname"`
	NumCPU     int     `json:"num_cpu"`
	Load1      float64 `json:"load1"`
	Load5      float64 `json:"load5"`
	Load15     float64 `json:"load15"`
	UptimeSec  float64 `json:"uptime_sec"`
	CPUPercent float64 `json:"cpu_percent"`
	MemTotal   uint64  `json:"mem_total"`
	MemUsed    uint64  `json:"mem_used"`
	SwapTotal  uint64  `json:"swap_total"`
	SwapUsed   uint64  `json:"swap_used"`
	DiskTotal  uint64  `json:"disk_total"`
	DiskUsed   uint64  `json:"disk_used"`
	ProcessCPU float64 `json:"process_cpu_percent"`
}

// Collect gathers a metrics snapshot. CPU percent is computed from two
// /proc/stat reads separated by interval (a single read can't express "%
// busy"), and process CPU from two /proc/self/stat reads over the same
// window.
func Collect(interval time.Duration) Sample {
	s := Sample{
		Hostname: hostname(),
		NumCPU:   runtime.NumCPU(),
	}
	if vals := readLoadavg(); len(vals) == 3 {
		s.Load1, s.Load5, s.Load15 = vals[0], vals[1], vals[2]
	}
	s.UptimeSec = readUptime()
	s.MemTotal, s.MemUsed, s.SwapTotal, s.SwapUsed = readMeminfo()
	s.DiskTotal, s.DiskUsed = readDisk()

	s.CPUPercent = cpuPercent(interval)
	s.ProcessCPU = processCPUPercent(interval)
	return s
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// parseFloatLine pulls N whitespace-separated floats out of a proc line.
func parseFloatLine(data, prefix string, max int) []float64 {
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line[len(prefix):])
		out := make([]float64, 0, max)
		for _, f := range fields {
			if len(out) == max {
				break
			}
			v, err := strconv.ParseFloat(f, 64)
			if err != nil {
				return nil
			}
			out = append(out, v)
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

func readLoadavg() []float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil
	}
	return parseFloatLine(string(data), "", 3)
}

func readUptime() float64 {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	vals := parseFloatLine(string(data), "", 2)
	if len(vals) == 0 {
		return 0
	}
	return vals[0]
}

func readMeminfo() (total, used, swapTotal, swapUsed uint64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0
	}
	return calcMem(data)
}

// calcMem turns /proc/meminfo bytes into the memory numbers. Split out of
// readMeminfo so tests can feed it synthetic samples.
func calcMem(data []byte) (total, used, swapTotal, swapUsed uint64) {
	if len(data) == 0 {
		return 0, 0, 0, 0
	}
	kb := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		kb[key] = v * 1024 // everything is in kB
	}
	total = kb["MemTotal"]
	available := kb["MemAvailable"]
	used = total - available
	swapTotal = kb["SwapTotal"]
	swapUsed = kb["SwapTotal"] - kb["SwapFree"]
	if swapTotal == 0 {
		swapUsed = 0
	}
	return total, used, swapTotal, swapUsed
}

// readCPUTimes parses the aggregated `cpu ` line of /proc/stat into
// (idle, total) jiffies.
func readCPUTimes() (idle, total uint64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line[3:])
		for i, f := range fields {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return 0, 0
			}
			// field 3 is idle, field 4 is iowait - both count as idle
			if i <= 3 {
				idle += v
			}
			total += v
		}
		return idle, total
	}
	return 0, 0
}

func cpuPercent(interval time.Duration) float64 {
	idle1, total1 := readCPUTimes()
	time.Sleep(interval)
	idle2, total2 := readCPUTimes()
	dTotal := total2 - total1
	if dTotal == 0 {
		return 0
	}
	dIdle := idle2 - idle1
	return 100 * (1 - float64(dIdle)/float64(dTotal))
}

// readSelfStat parses /proc/self/stat: field 14 (utime) and 15 (stime) are
// the process's CPU jiffies.
func readSelfStat() (ticks, start uint64) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, 0
	}
	// The comm field (field 2) can contain spaces and parens, so split on
	// the LAST ')' instead of whitespace.
	rest := string(data)
	if idx := strings.LastIndex(rest, ")"); idx >= 0 {
		rest = rest[idx+2:] // skip ") " separator
	}
	fields := strings.Fields(rest)
	if len(fields) < 22 {
		return 0, 0
	}
	utime, err1 := strconv.ParseUint(fields[11], 10, 64) // field 14 overall
	stime, err2 := strconv.ParseUint(fields[12], 10, 64) // field 15 overall
	start, err3 := strconv.ParseUint(fields[19], 10, 64) // field 22 overall
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, 0
	}
	return utime + stime, start
}

func processCPUPercent(interval time.Duration) float64 {
	cpus := runtime.NumCPU()
	ticks1, _ := readSelfStat()
	time.Sleep(interval)
	ticks2, _ := readSelfStat()
	dTicks := ticks2 - ticks1
	// 100 system clock ticks per second; scale to a *single-core* percent
	// so >100% is impossible to confuse with >all-cores usage.
	coreSeconds := interval.Seconds() * 100 * float64(cpus)
	if coreSeconds == 0 {
		return 0
	}
	return 100 * float64(dTicks) / coreSeconds
}

func readDisk() (total, used uint64) {
	var st statfsT
	if statfsPath("/", &st) != nil || st.Blocks == 0 {
		return 0, 0
	}
	total = st.Blocks * uint64(st.Bsize)
	free := st.Bfree * uint64(st.Bsize)
	used = total - free
	return total, used
}
