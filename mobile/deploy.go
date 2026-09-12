package mobile

import (
	"encoding/json"
	"fmt"

	"universal-bypass-tool/deployssh"
)

// SSHTargetJSON/DeployOptionsJSON mirror deployssh.SSHTarget/DeployOptions
// field-for-field. Deploy takes them as JSON strings rather than gomobile-
// bound struct types: gomobile silently drops any top-level function that
// takes a custom struct as a parameter (confirmed empirically - it doesn't
// even error, the function just never appears in the generated binding),
// so passing JSON and unmarshaling it here follows the same pattern
// StartTunnel already uses for its own Config.
type sshTargetJSON struct {
	Host                    string `json:"host"`
	Port                    int    `json:"port"`
	Username                string `json:"username"`
	AuthMethod              string `json:"auth_method"` // "password" or "key"
	Password                string `json:"password"`
	PrivateKeyPEM           string `json:"private_key_pem"`
	Passphrase              string `json:"passphrase"`
	KnownHostKeyFingerprint string `json:"known_host_key_fingerprint"`
}

type deployOptionsJSON struct {
	DeployScriptURL string `json:"deploy_script_url"`
	RepoURL         string `json:"repo_url"`
	GitRef          string `json:"git_ref"`
	TLSMode         string `json:"tls_mode"` // "domain" or "ip"
	Domain          string `json:"domain"`
	Email           string `json:"email"`
	ServerIP        string `json:"server_ip"`
	AdminToken      string `json:"admin_token"`
	DBPassword      string `json:"db_password"`
	RegisterNode    bool   `json:"register_node"`
	NodeName        string `json:"node_name"`
	NodeMaxKeys     int    `json:"node_max_keys"`
	RunNodeHere     bool   `json:"run_node_here"`
}

// DeployCallback receives live progress from Deploy. Implemented on the
// Kotlin side, same pattern as Callback in mobile.go - unlike structs,
// interfaces work fine as direct gomobile function parameters.
type DeployCallback interface {
	OnLog(line string)
	OnHostKeyFingerprint(fingerprint string)
	OnDeployResult(panelURL, adminToken, nodeToken string)
}

// Deploy runs openflux-deploy's install.sh on the target described by
// targetJSON over SSH, non-interactively, with optsJSON's fields pre-set as
// environment variables - see deployssh.Deploy for the actual
// implementation and deployssh.SSHTarget/DeployOptions for the JSON shape
// (snake_case field names, as shown above). Blocks until the remote script
// finishes.
func Deploy(targetJSON string, optsJSON string, cb DeployCallback) error {
	var target sshTargetJSON
	if err := json.Unmarshal([]byte(targetJSON), &target); err != nil {
		return fmt.Errorf("parse target: %w", err)
	}
	var opts deployOptionsJSON
	if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
		return fmt.Errorf("parse options: %w", err)
	}

	return deployssh.Deploy(
		deployssh.SSHTarget{
			Host:                    target.Host,
			Port:                    target.Port,
			Username:                target.Username,
			AuthMethod:              target.AuthMethod,
			Password:                target.Password,
			PrivateKeyPEM:           target.PrivateKeyPEM,
			Passphrase:              target.Passphrase,
			KnownHostKeyFingerprint: target.KnownHostKeyFingerprint,
		},
		deployssh.DeployOptions{
			DeployScriptURL: opts.DeployScriptURL,
			RepoURL:         opts.RepoURL,
			GitRef:          opts.GitRef,
			TLSMode:         opts.TLSMode,
			Domain:          opts.Domain,
			Email:           opts.Email,
			ServerIP:        opts.ServerIP,
			AdminToken:      opts.AdminToken,
			DBPassword:      opts.DBPassword,
			RegisterNode:    opts.RegisterNode,
			NodeName:        opts.NodeName,
			NodeMaxKeys:     opts.NodeMaxKeys,
			RunNodeHere:     opts.RunNodeHere,
		},
		deployCallbackAdapter{cb},
	)
}

// deployCallbackAdapter satisfies deployssh.Callback by forwarding to the
// gomobile-bound DeployCallback.
type deployCallbackAdapter struct {
	cb DeployCallback
}

func (a deployCallbackAdapter) OnLog(line string) {
	if a.cb != nil {
		a.cb.OnLog(line)
	}
}

func (a deployCallbackAdapter) OnHostKeyFingerprint(fingerprint string) {
	if a.cb != nil {
		a.cb.OnHostKeyFingerprint(fingerprint)
	}
}

func (a deployCallbackAdapter) OnDeployResult(panelURL, adminToken, nodeToken string) {
	if a.cb != nil {
		a.cb.OnDeployResult(panelURL, adminToken, nodeToken)
	}
}
