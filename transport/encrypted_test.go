package transport

import "testing"

// pipeTransport is an in-memory Transport whose Send delivers straight to
// whatever callback the other end's Receive registered - just enough to
// test EncryptedTransport's Send/Receive without a real network.
type pipeTransport struct {
	Transport
	peer *pipeTransport
	cb   func([]byte)
}

func (p *pipeTransport) Send(data []byte) error {
	if p.peer != nil && p.peer.cb != nil {
		p.peer.cb(data)
	}
	return nil
}

func (p *pipeTransport) Receive(callback func([]byte)) { p.cb = callback }

func newPipe() (a, b *pipeTransport) {
	a, b = &pipeTransport{}, &pipeTransport{}
	a.peer, b.peer = b, a
	return
}

func TestEncryptedTransportRoundTrip(t *testing.T) {
	rawA, rawB := newPipe()
	client := NewEncryptedTransport(rawA, "shared-secret-token", false)
	node := NewEncryptedTransport(rawB, "shared-secret-token", true)

	var gotAtNode, gotAtClient []byte
	node.Receive(func(d []byte) { gotAtNode = d })
	client.Receive(func(d []byte) { gotAtClient = d })

	if err := client.Send([]byte("hello from client")); err != nil {
		t.Fatalf("client.Send: %v", err)
	}
	if string(gotAtNode) != "hello from client" {
		t.Errorf("node received %q, want %q", gotAtNode, "hello from client")
	}

	if err := node.Send([]byte("hello from node")); err != nil {
		t.Fatalf("node.Send: %v", err)
	}
	if string(gotAtClient) != "hello from node" {
		t.Errorf("client received %q, want %q", gotAtClient, "hello from node")
	}
}

func TestEncryptedTransportManyPacketsInOrder(t *testing.T) {
	rawA, rawB := newPipe()
	client := NewEncryptedTransport(rawA, "shared-secret-token", false)
	node := NewEncryptedTransport(rawB, "shared-secret-token", true)

	var got [][]byte
	node.Receive(func(d []byte) { got = append(got, append([]byte(nil), d...)) })

	for i := 0; i < 50; i++ {
		if err := client.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
	}
	if len(got) != 50 {
		t.Fatalf("got %d packets, want 50", len(got))
	}
	for i, p := range got {
		if len(p) != 1 || p[0] != byte(i) {
			t.Errorf("packet %d = %v, want [%d]", i, p, i)
		}
	}
}

// The client side (isExitNode=false) never auto-falls-back to plaintext, so
// a wrong token there just drops the packet - unlike the exit-node side,
// which can't tell "wrong token" from "peer never encrypted" (see
// TestEncryptedTransportExitNodePassesThroughUnencryptedPeer) and must
// assume the latter.
func TestEncryptedTransportWrongTokenFailsToDecrypt(t *testing.T) {
	rawA, rawB := newPipe()
	node := NewEncryptedTransport(rawA, "token-one", true)
	client := NewEncryptedTransport(rawB, "token-two", false)

	var got []byte
	client.Receive(func(d []byte) { got = d })
	node.Send([]byte("secret"))

	if got != nil {
		t.Errorf("decrypted with the wrong token: %v", got)
	}
}

// An old app build, or a different app entirely, never wraps its transport
// in EncryptedTransport at all - so from the exit node's side this looks
// exactly like plain, unencrypted packets arriving on the wire. The node
// must keep delivering them instead of dropping real traffic.
func TestEncryptedTransportExitNodePassesThroughUnencryptedPeer(t *testing.T) {
	rawA, rawB := newPipe()
	node := NewEncryptedTransport(rawA, "shared-secret-token", true)

	var got []byte
	node.Receive(func(d []byte) { got = d })
	rawB.Send([]byte("plaintext from an old client"))

	if string(got) != "plaintext from an old client" {
		t.Errorf("got %q, want plaintext passed through", got)
	}
}

// Until the node has seen proof the peer understands encryption, it must
// keep sending plaintext too - otherwise an old/foreign peer would never
// understand the node's replies either.
func TestEncryptedTransportExitNodeSendsPlaintextUntilPeerProvesEncryption(t *testing.T) {
	rawA, rawB := newPipe()
	node := NewEncryptedTransport(rawA, "shared-secret-token", true)

	var gotRaw []byte
	rawB.Receive(func(d []byte) { gotRaw = d })
	if err := node.Send([]byte("hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if string(gotRaw) != "hello" {
		t.Errorf("node sent %q before seeing ciphertext, want plaintext passthrough", gotRaw)
	}

	client := NewEncryptedTransport(rawB, "shared-secret-token", false)
	var gotAtNode []byte
	node.Receive(func(d []byte) { gotAtNode = d })
	if err := client.Send([]byte("now encrypting")); err != nil {
		t.Fatalf("client.Send: %v", err)
	}
	if string(gotAtNode) != "now encrypting" {
		t.Fatalf("node failed to decrypt client's first ciphertext: %q", gotAtNode)
	}

	rawB.Receive(func(d []byte) { gotRaw = d })
	if err := node.Send([]byte("now switching to ciphertext too")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if string(gotRaw) == "now switching to ciphertext too" {
		t.Errorf("node still sent plaintext after seeing the peer encrypt")
	}
}

func TestEncryptedTransportDirectionKeysDiffer(t *testing.T) {
	inner, _ := newPipe()
	client := NewEncryptedTransport(inner, "shared-secret-token", false)
	node := NewEncryptedTransport(inner, "shared-secret-token", true)

	if client.send == node.send {
		t.Errorf("client and node ended up with the same send key")
	}
	if client.send != node.recv || client.recv != node.send {
		t.Errorf("client/node send-recv keys don't line up: client.send=%v node.recv=%v", client.send, node.recv)
	}
}
