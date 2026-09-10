package gateway

import (
	"bytes"
	"testing"
)

func TestWriteReadDNSOverTCPRoundTrip(t *testing.T) {
	msg := []byte("fake dns message payload")

	var buf bytes.Buffer
	if err := writeDNSOverTCP(&buf, msg); err != nil {
		t.Fatalf("writeDNSOverTCP: %v", err)
	}

	got, err := readDNSOverTCP(&buf)
	if err != nil {
		t.Fatalf("readDNSOverTCP: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("round trip = %q, want %q", got, msg)
	}
}

func TestWriteDNSOverTCPRejectsOversizedMessage(t *testing.T) {
	huge := make([]byte, maxDNSMessageSize+1)

	var buf bytes.Buffer
	if err := writeDNSOverTCP(&buf, huge); err == nil {
		t.Fatalf("expected an error for a message over %d bytes", maxDNSMessageSize)
	}
}

func TestReadDNSOverTCPTruncatedLength(t *testing.T) {
	// Only one byte of the two-byte length prefix, then nothing.
	buf := bytes.NewReader([]byte{0x00})
	if _, err := readDNSOverTCP(buf); err == nil {
		t.Fatalf("expected an error reading a truncated length prefix")
	}
}
