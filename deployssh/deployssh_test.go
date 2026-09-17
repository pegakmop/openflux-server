package deployssh

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// --- pure-logic tests: no network needed --------------------------------

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"plain":     `'plain'`,
		"has space": `'has space'`,
		"":          `''`,
		"a'b":       `'a'\''b'`,
		"a'b'c":     `'a'\''b'\''c'`,
	}
	for in, want := range cases {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildRemoteCommandIncludesDefaultsAndQuoting(t *testing.T) {
	cmd := buildRemoteCommand(DeployOptions{
		RepoURL:      "https://github.com/wlruscfd/openflux-server.git",
		GitRef:       "main",
		TLSMode:      "domain",
		Domain:       "panel.example.com",
		Email:        "you@example.com",
		AdminToken:   "adm'in",
		DBPassword:   "dbpass",
		RegisterNode: false,
	})

	if !strings.Contains(cmd, "curl -fsSL '"+defaultDeployScriptURL+"?_=") {
		t.Errorf("command should curl the default script URL (cache-busted) when none is set: %s", cmd)
	}
	if !strings.Contains(cmd, `export ADMIN_TOKEN='adm'\''in';`) {
		t.Errorf("ADMIN_TOKEN should be shell-quoted with the embedded quote escaped: %s", cmd)
	}
	if !strings.Contains(cmd, "export REGISTER_NODE='n';") {
		t.Errorf("REGISTER_NODE=n must be explicit (not omitted) so it overrides install.sh's own default: %s", cmd)
	}
	if !strings.Contains(cmd, "export RUN_NODE_HERE='n';") {
		t.Errorf("RUN_NODE_HERE=n must be explicit (not omitted) so it overrides install.sh's own default: %s", cmd)
	}
	if !strings.Contains(cmd, "export WEB_PANEL='n';") {
		t.Errorf("WEB_PANEL='n' must be explicit - this exec has no controlling terminal, so install.sh's own prompt would silently fall through to its own 'y' default and attempt a Bun/SvelteKit build no one asked for: %s", cmd)
	}
	if strings.Contains(cmd, "SERVER_IP") {
		t.Errorf("SERVER_IP is blank in domain mode and should not appear at all: %s", cmd)
	}
	if !strings.HasSuffix(cmd, "bash /tmp/openflux-install.sh") {
		t.Errorf("command should end by running the fetched script: %s", cmd)
	}
}

func TestBuildRemoteCommandCustomScriptURL(t *testing.T) {
	cmd := buildRemoteCommand(DeployOptions{DeployScriptURL: "https://example.com/my-install.sh"})
	if !strings.Contains(cmd, "curl -fsSL 'https://example.com/my-install.sh?_=") {
		t.Errorf("a custom DeployScriptURL should be used, cache-busted the same way as the default: %s", cmd)
	}
}

func TestCacheBustAppendsQueryParamCorrectly(t *testing.T) {
	if got := cacheBust("https://example.com/install.sh"); !strings.HasPrefix(got, "https://example.com/install.sh?_=") {
		t.Errorf("cacheBust(no existing query) = %q, want a ?_= param appended", got)
	}
	if got := cacheBust("https://example.com/install.sh?ref=main"); !strings.HasPrefix(got, "https://example.com/install.sh?ref=main&_=") {
		t.Errorf("cacheBust(existing query) = %q, want an &_= param appended", got)
	}
}

// --- real local SSH server tests -----------------------------------------

type testCallback struct {
	mu          sync.Mutex
	lines       []string
	fingerprint string
	result      *deployResult
}

type deployResult struct {
	panelURL, adminToken, nodeToken string
}

func (c *testCallback) OnLog(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
}

func (c *testCallback) OnHostKeyFingerprint(fp string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fingerprint = fp
}

func (c *testCallback) OnDeployResult(panelURL, adminToken, nodeToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.result = &deployResult{panelURL: panelURL, adminToken: adminToken, nodeToken: nodeToken}
}

func (c *testCallback) Lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *testCallback) Result() *deployResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.result
}

func generateHostSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("signer from host key: %v", err)
	}
	return signer
}

func generateRSAClientKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})

	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("signer from rsa key: %v", err)
	}
	return signer.PublicKey(), string(pemBytes)
}

// startTestSSHServer runs one exec session per connection: it decodes the
// "exec" request's command, hands it to handle (which writes to the
// session's stdout/stderr and returns an exit code), then closes.
func startTestSSHServer(t *testing.T, config *ssh.ServerConfig, hostSigner ssh.Signer, handle func(cmd string, stdout, stderr io.Writer) int) string {
	t.Helper()
	config.AddHostKey(hostSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			nConn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveOneConn(nConn, config, handle)
		}
	}()

	return listener.Addr().String()
}

func serveOneConn(nConn net.Conn, config *ssh.ServerConfig, handle func(cmd string, stdout, stderr io.Writer) int) {
	sshConn, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		return // auth failure etc - expected for the negative-path tests
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go func() {
			defer channel.Close()
			for req := range requests {
				if req.Type != "exec" {
					req.Reply(false, nil)
					continue
				}
				var payload struct{ Command string }
				ssh.Unmarshal(req.Payload, &payload)
				req.Reply(true, nil)

				code := handle(payload.Command, channel, channel.Stderr())
				channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(code)}))
				return
			}
		}()
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port %q: %v", addr, err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func TestDeploySuccessStreamsOutputAndFingerprint(t *testing.T) {
	hostSigner := generateHostSigner(t)
	var gotCmd string

	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == "root" && string(pass) == "correct-horse" {
				return nil, nil
			}
			return nil, fmt.Errorf("denied")
		},
	}

	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		gotCmd = cmd
		fmt.Fprintln(stdout, "installing packages")
		fmt.Fprintln(stdout, "done")
		fmt.Fprintln(stderr, "a warning on stderr")
		return 0
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "correct-horse"}
	opts := DeployOptions{RepoURL: "https://x/y.git", GitRef: "main", TLSMode: "ip", ServerIP: "1.2.3.4", AdminToken: "tok"}

	if err := Deploy(target, opts, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	wantFP := ssh.FingerprintSHA256(hostSigner.PublicKey())
	if cb.fingerprint != wantFP {
		t.Errorf("fingerprint = %q, want %q", cb.fingerprint, wantFP)
	}

	lines := cb.Lines()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"installing packages", "done", "a warning on stderr"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected log output to contain %q, got: %v", want, lines)
		}
	}

	if !strings.Contains(gotCmd, "export SERVER_IP='1.2.3.4';") {
		t.Errorf("remote command should carry SERVER_IP for ip mode: %s", gotCmd)
	}
}

func TestDeployWrongPasswordFails(t *testing.T) {
	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			return nil, fmt.Errorf("denied")
		},
	}
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		t.Errorf("command should never run when auth fails")
		return 0
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "wrong"}
	if err := Deploy(target, DeployOptions{}, cb); err == nil {
		t.Fatalf("expected an error for a rejected password")
	}
}

func TestDeployHostKeyMismatchRejectsBeforeRunningAnything(t *testing.T) {
	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	ran := false
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		ran = true
		return 0
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{
		Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x",
		KnownHostKeyFingerprint: "SHA256:this-will-never-match-anything",
	}
	err := Deploy(target, DeployOptions{}, cb)
	if err == nil {
		t.Fatalf("expected an error for a mismatched host key")
	}
	if ran {
		t.Errorf("the remote command must not run when the host key doesn't match")
	}
}

func TestDeployKeyAuth(t *testing.T) {
	hostSigner := generateHostSigner(t)
	clientPub, clientPEM := generateRSAClientKey(t)

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if bytes.Equal(pubKey.Marshal(), clientPub.Marshal()) {
				return nil, nil
			}
			return nil, fmt.Errorf("unauthorized key")
		},
	}
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		fmt.Fprintln(stdout, "ok")
		return 0
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "key", PrivateKeyPEM: clientPEM}
	if err := Deploy(target, DeployOptions{}, cb); err != nil {
		t.Fatalf("Deploy with key auth: %v", err)
	}
}

func TestDeploySuccessReportsResultLineAndHidesItFromLog(t *testing.T) {
	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		fmt.Fprintln(stdout, "installing packages")
		fmt.Fprintln(stdout, "OPENFLUX_DEPLOY_RESULT panel_url=https://1.2.3.4/admin/ admin_token=admtok node_token=nodetok")
		fmt.Fprintln(stdout, "done")
		return 0
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	result := cb.Result()
	if result == nil {
		t.Fatalf("OnDeployResult was never called")
	}
	if result.panelURL != "https://1.2.3.4/admin/" || result.adminToken != "admtok" || result.nodeToken != "nodetok" {
		t.Errorf("result = %+v, want panel_url=https://1.2.3.4/admin/ admin_token=admtok node_token=nodetok", *result)
	}

	for _, line := range cb.Lines() {
		if strings.Contains(line, "OPENFLUX_DEPLOY_RESULT") {
			t.Errorf("the machine-readable result line should not also be reported via OnLog: %q", line)
		}
	}
}

func TestDeployNonZeroExitIsAnError(t *testing.T) {
	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		fmt.Fprintln(stderr, "something went wrong")
		return 1
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, cb); err == nil {
		t.Fatalf("expected an error when the remote script exits non-zero")
	}
}

// --- keepalive --------------------------------------------------------

// golang.org/x/crypto/ssh sends no keepalive traffic of its own, so a remote
// command producing no output for a while can sit on an idle connection long
// enough for a NAT/firewall to drop it. Shrinks keepaliveInterval and asserts
// at least one keepalive reaches the server during a silent remote command.
func TestDeploySendsKeepaliveDuringQuietRemoteCommand(t *testing.T) {
	old := keepaliveInterval
	keepaliveInterval = 20 * time.Millisecond
	defer func() { keepaliveInterval = old }()

	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	config.AddHostKey(hostSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	var mu sync.Mutex
	keepaliveCount := 0

	go func() {
		for {
			nConn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				sshConn, chans, reqs, err := ssh.NewServerConn(nConn, config)
				if err != nil {
					return
				}
				defer sshConn.Close()
				go func() {
					for req := range reqs {
						if req.Type == "keepalive@openflux" {
							mu.Lock()
							keepaliveCount++
							mu.Unlock()
						}
						if req.WantReply {
							req.Reply(true, nil)
						}
					}
				}()
				for newChannel := range chans {
					if newChannel.ChannelType() != "session" {
						newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
						continue
					}
					channel, requests, err := newChannel.Accept()
					if err != nil {
						continue
					}
					go func() {
						defer channel.Close()
						for req := range requests {
							if req.Type != "exec" {
								req.Reply(false, nil)
								continue
							}
							req.Reply(true, nil)
							// Quiet for several keepalive intervals - no
							// output at all - before finishing, mirroring
							// install.sh's silent `go build` steps.
							time.Sleep(200 * time.Millisecond)
							channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
							return
						}
					}()
				}
			}()
		}
	}()

	host, port := splitHostPort(t, listener.Addr().String())
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, &testCallback{}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	mu.Lock()
	got := keepaliveCount
	mu.Unlock()
	if got == 0 {
		t.Errorf("no keepalive requests reached the server during a quiet remote command")
	}
}
