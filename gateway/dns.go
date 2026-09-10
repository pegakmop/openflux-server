package gateway

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"universal-bypass-tool/utils"
)

const (
	dnsQueryTimeout   = 8 * time.Second
	maxDNSMessageSize = 65535
)

// relayDNS handles exactly one DNS query arriving on a "connected" UDP
// endpoint (its peer is fixed to whichever app sent the original query): it
// forwards the query to dnsUpstream over TCP using the RFC 1035 DNS-over-TCP
// framing, via the same Dialer used for ordinary TCP traffic - so
// resolution goes through the covert tunnel like everything else - and
// writes the single reply back as one UDP datagram.
func (s *Server) relayDNS(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(dnsQueryTimeout))

	query := make([]byte, maxDNSMessageSize)
	n, err := conn.Read(query)
	if err != nil {
		utils.Debugf("[GATEWAY] dns query read failed: %v", err)
		return
	}
	query = query[:n]

	upstream, err := s.dialer.DialTCP(s.dnsUpstream)
	if err != nil {
		utils.Debugf("[GATEWAY] dns upstream dial failed: %v", err)
		return
	}
	defer upstream.Close()
	upstream.SetDeadline(time.Now().Add(dnsQueryTimeout))

	if err := writeDNSOverTCP(upstream, query); err != nil {
		utils.Debugf("[GATEWAY] dns upstream write failed: %v", err)
		return
	}

	reply, err := readDNSOverTCP(upstream)
	if err != nil {
		utils.Debugf("[GATEWAY] dns upstream read failed: %v", err)
		return
	}

	if _, err := conn.Write(reply); err != nil {
		utils.Debugf("[GATEWAY] dns reply write failed: %v", err)
	}
}

func writeDNSOverTCP(w io.Writer, msg []byte) error {
	if len(msg) > maxDNSMessageSize {
		return fmt.Errorf("dns message too large: %d bytes", len(msg))
	}
	var lenPrefix [2]byte
	binary.BigEndian.PutUint16(lenPrefix[:], uint16(len(msg)))
	if _, err := w.Write(lenPrefix[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}

func readDNSOverTCP(r io.Reader) ([]byte, error) {
	var lenPrefix [2]byte
	if _, err := io.ReadFull(r, lenPrefix[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint16(lenPrefix[:])
	msg := make([]byte, size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
