package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr     string
	DatabaseURL    string
	TokenPepper    string
	AdminToken     string
	NodePollWindow time.Duration
	RateLimitRPS   float64
	RateLimitBurst int
	// PublicBaseURL is how clients reach this controlplane (e.g.
	// https://your-domain-or-ip - no path, no trailing slash) - not the
	// same thing as ListenAddr, which is only the internal bind address.
	// Optional: unset just means admin-created keys don't get a deep link
	// in their API response (see internal/api/deeplink.go).
	PublicBaseURL string
}

func FromEnv() (Config, error) {
	cfg := Config{
		ListenAddr:     getEnv("CONTROLPLANE_LISTEN_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("CONTROLPLANE_DATABASE_URL"),
		TokenPepper:    os.Getenv("CONTROLPLANE_TOKEN_PEPPER"),
		AdminToken:     os.Getenv("CONTROLPLANE_ADMIN_TOKEN"),
		NodePollWindow: 90 * time.Second,
		RateLimitRPS:   1,
		RateLimitBurst: 5,
		PublicBaseURL:  strings.TrimRight(os.Getenv("CONTROLPLANE_PUBLIC_URL"), "/"),
	}

	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("CONTROLPLANE_DATABASE_URL is required")
	}
	if cfg.TokenPepper == "" {
		return cfg, fmt.Errorf("CONTROLPLANE_TOKEN_PEPPER is required")
	}
	if cfg.AdminToken == "" {
		return cfg, fmt.Errorf("CONTROLPLANE_ADMIN_TOKEN is required")
	}

	if v := os.Getenv("CONTROLPLANE_RATE_LIMIT_RPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.RateLimitRPS = f
		}
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
