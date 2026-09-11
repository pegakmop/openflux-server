package config

import "testing"

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CONTROLPLANE_DATABASE_URL", "postgres://x/y")
	t.Setenv("CONTROLPLANE_TOKEN_PEPPER", "pepper")
	t.Setenv("CONTROLPLANE_ADMIN_TOKEN", "admintoken")
}

func TestFromEnvPublicBaseURLDefaultsEmpty(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.PublicBaseURL != "" {
		t.Errorf("PublicBaseURL = %q, want empty when CONTROLPLANE_PUBLIC_URL is unset", cfg.PublicBaseURL)
	}
}

func TestFromEnvPublicBaseURLTrimsTrailingSlash(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("CONTROLPLANE_PUBLIC_URL", "https://example.com/")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.PublicBaseURL != "https://example.com" {
		t.Errorf("PublicBaseURL = %q, want %q", cfg.PublicBaseURL, "https://example.com")
	}
}

func TestFromEnvMissingRequiredFields(t *testing.T) {
	if _, err := FromEnv(); err == nil {
		t.Fatalf("expected an error when no required env vars are set")
	}
}
