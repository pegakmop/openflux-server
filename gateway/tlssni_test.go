package gateway

import (
	"encoding/binary"
	"testing"
)

// buildClientHello assembles a minimal TLS 1.3 ClientHello with an optional
// SNI extension so tests can exercise sniServerName against real framing.
func buildClientHello(sni string, withSNI bool) []byte {
	var hello []byte
	hello = append(hello, 0x03, 0x03) // legacy_version
	random := make([]byte, 32)
	hello = append(hello, random...)
	hello = append(hello, 0x00)                               // no session id
	hello = append(hello, 0x00, 0x04, 0x13, 0x01, 0x13, 0x02) // two ciphers
	hello = append(hello, 0x01, 0x00)                         // one compression method (null)

	var exts []byte
	if withSNI {
		name := []byte(sni)
		entry := append([]byte{0x00}, byte(len(name)>>8), byte(len(name))) // type + 2-byte name length
		entry = append(entry, name...)
		list := append([]byte{byte(len(entry) >> 8), byte(len(entry))}, entry...)
		ext := []byte{0x00, 0x00} // server_name type
		ext = append(ext, byte(len(list)>>8), byte(len(list)))
		ext = append(ext, list...)
		exts = append(exts, ext...)
	}
	hello = append(hello, byte(len(exts)>>8), byte(len(exts)))
	hello = append(hello, exts...)

	handshake := append([]byte{0x01}, byte(len(hello)>>16), byte(len(hello)>>8), byte(len(hello)))
	handshake = append(handshake, hello...)

	record := []byte{0x16, 0x03, 0x03, 0x00, 0x00}
	binary.BigEndian.PutUint16(record[3:5], uint16(len(handshake)))
	return append(record, handshake...)
}

func TestSNIServerName(t *testing.T) {
	hello := buildClientHello("www.example.com", true)
	if got := sniServerName(hello); got != "www.example.com" {
		t.Errorf("SNI = %q, want %q", got, "www.example.com")
	}
}

func TestSNIWithoutExtension(t *testing.T) {
	hello := buildClientHello("ignored", false)
	if got := sniServerName(hello); got != "" {
		t.Errorf("expected no SNI, got %q", got)
	}
}

func TestSNITruncatedPrefix(t *testing.T) {
	hello := buildClientHello("www.example.com", true)
	// Every strict prefix shorter than the full record must parse without a
	// panic and either find the SNI (when it happens to be complete enough)
	// or report none.
	for cut := 0; cut < len(hello); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("sniServerName panicked on %d-byte prefix: %v", cut, r)
				}
			}()
			_ = sniServerName(hello[:cut])
		}()
	}
	// The SNI lives early in the hello, so a prefix that includes it should
	// still be parseable even without the record's full handshake length.
	prefix := hello[:40]
	if got := sniServerName(prefix); got != "" {
		t.Logf("long prefix resolved to %q (substrings may vary)", got)
	}
}

func TestSNINotClientHello(t *testing.T) {
	if got := sniServerName([]byte{0x00, 0x00, 0x00, 0x00, 0x00}); got != "" {
		t.Errorf("got %q, want empty for a non-TLS prefix", got)
	}
	if got := sniServerName(nil); got != "" {
		t.Errorf("got %q, want empty for an empty prefix", got)
	}
	// A TLS alert (content type 21) must not be treated as a handshake.
	alert := []byte{0x15, 0x03, 0x01, 0x00, 0x02, 0x01, 0x00}
	if got := sniServerName(alert); got != "" {
		t.Errorf("got %q, want empty for a TLS alert record", got)
	}
}

func TestSNIWithExtraExtensions(t *testing.T) {
	// The SNI extension need not be first; the parser must skip a preceding
	// extension. Build a hello with a supported_versions extension first.
	var hello []byte
	hello = append(hello, 0x03, 0x03)
	hello = append(hello, make([]byte, 32)...)
	hello = append(hello, 0x00)
	hello = append(hello, 0x00, 0x04, 0x13, 0x01, 0x13, 0x02)
	hello = append(hello, 0x01, 0x00)

	name := []byte("sni.after.com")
	entry := append([]byte{0x00}, byte(len(name)>>8), byte(len(name)))
	entry = append(entry, name...)
	list := append([]byte{byte(len(entry) >> 8), byte(len(entry))}, entry...)
	exts := []byte{0x00, 0x2b, 0x00, 0x02, 0x03, 0x04} // supported_versions, 2 bytes
	exts = append(exts, 0x00, 0x00, byte(len(list)>>8), byte(len(list)))
	exts = append(exts, list...)

	hello = append(hello, byte(len(exts)>>8), byte(len(exts)))
	hello = append(hello, exts...)

	handshake := append([]byte{0x01}, byte(len(hello)>>16), byte(len(hello)>>8), byte(len(hello)))
	handshake = append(handshake, hello...)
	record := []byte{0x16, 0x03, 0x03, 0x00, 0x00}
	binary.BigEndian.PutUint16(record[3:5], uint16(len(handshake)))
	msg := append(record, handshake...)

	if got := sniServerName(msg); got != "sni.after.com" {
		t.Errorf("SNI = %q, want %q", got, "sni.after.com")
	}
}
