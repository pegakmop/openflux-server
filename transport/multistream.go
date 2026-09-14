package transport

import "sync"

// MultiStreamTransport spreads one tunnel's traffic across several
// independent underlying transports (e.g. several Yandex Docs sessions) to
// get past the throughput ceiling of a single one - each stream is its own
// real TCP-backed connection, so N streams give roughly N times the window
// a single connection is limited to.
//
// Routing is by flow, not round-robin: every packet's (src port, dst port)
// pair picks the same stream for the life of that flow (see streamIndex),
// so one real TCP connection's segments always travel one physical stream
// and arrive in order. This only multiplies throughput across CONCURRENT
// flows (parallel connections, as a browser or a downloader already opens)
// - a single flow never spans more than one stream and is bounded by that
// one stream's own ceiling.
type MultiStreamTransport struct {
	streams []Transport

	mu       sync.RWMutex
	callback func([]byte)
}

func NewMultiStreamTransport(streams []Transport) *MultiStreamTransport {
	m := &MultiStreamTransport{streams: streams}
	for _, s := range streams {
		s.Receive(func(data []byte) {
			m.mu.RLock()
			cb := m.callback
			m.mu.RUnlock()
			if cb != nil {
				cb(data)
			}
		})
	}
	return m
}

func (m *MultiStreamTransport) Start() error {
	for i, s := range m.streams {
		if err := s.Start(); err != nil {
			for _, prev := range m.streams[:i] {
				prev.Stop()
			}
			return err
		}
	}
	return nil
}

func (m *MultiStreamTransport) Stop() error {
	for _, s := range m.streams {
		s.Stop()
	}
	return nil
}

func (m *MultiStreamTransport) Send(data []byte) error {
	return m.streams[streamIndex(data, len(m.streams))].Send(data)
}

// streamIndex picks a stream by hashing the packet's two port fields, which
// sit at the same offset (bytes 0-3 of the L4 header) for both TCP and UDP.
// XOR is used instead of concatenation so the result is identical from
// either direction of a flow - a reply has src/dst swapped relative to the
// request, and XOR doesn't care which order its inputs came in.
func streamIndex(data []byte, n int) int {
	if n <= 1 {
		return 0
	}
	if len(data) < 20 {
		return 0
	}
	ipHeaderLen := int(data[0]&0x0F) * 4
	if len(data) < ipHeaderLen+4 {
		return 0
	}
	l4 := data[ipHeaderLen:]
	srcPort := uint16(l4[0])<<8 | uint16(l4[1])
	dstPort := uint16(l4[2])<<8 | uint16(l4[3])
	return int(srcPort^dstPort) % n
}

func (m *MultiStreamTransport) Receive(callback func([]byte)) {
	m.mu.Lock()
	m.callback = callback
	m.mu.Unlock()
}

// IsConnected reports true as long as at least one stream is up - the
// tunnel keeps carrying traffic (on fewer streams) rather than being torn
// down over one stream's transient reconnect.
func (m *MultiStreamTransport) IsConnected() bool {
	for _, s := range m.streams {
		if s.IsConnected() {
			return true
		}
	}
	return false
}

func (m *MultiStreamTransport) Stats() TransportStats {
	var out TransportStats
	for _, s := range m.streams {
		st := s.Stats()
		out.BytesSent += st.BytesSent
		out.BytesReceived += st.BytesReceived
		out.PacketsSent += st.PacketsSent
		out.PacketsRecv += st.PacketsRecv
		out.Reconnects += st.Reconnects
		if st.Uptime > out.Uptime {
			out.Uptime = st.Uptime
		}
	}
	out.Connected = m.IsConnected()
	return out
}

func (m *MultiStreamTransport) SetEventCallback(fn func(code, detail string)) {
	for _, s := range m.streams {
		s.SetEventCallback(fn)
	}
}
