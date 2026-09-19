package model

import "time"

type Node struct {
	ID              string
	Name            string
	MaxKeys         int
	Status          string
	LastHeartbeatAt *time.Time
	CreatedAt       time.Time
	ActiveKeys      int
}

type Key struct {
	ID                 string
	Label              string
	Transport          string
	DocURL             string
	DocURLs            []string
	E2EEncryption      bool
	AssignedNodeID     *string
	Enabled            bool
	TrafficLimitBytes  *int64
	BytesSentTotal     int64
	BytesReceivedTotal int64
	OwnerRef           string
	ExpiresAt          *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	LastSeenAt         *time.Time
}

func (k Key) BytesUsedTotal() int64 {
	return k.BytesSentTotal + k.BytesReceivedTotal
}

func (k Key) Status(now time.Time) string {
	if !k.Enabled {
		return "disabled"
	}
	if k.ExpiresAt != nil && now.After(*k.ExpiresAt) {
		return "disabled"
	}
	if k.TrafficLimitBytes != nil && k.BytesUsedTotal() >= *k.TrafficLimitBytes {
		return "over_quota"
	}
	return "active"
}

type IngestToken struct {
	ID        string
	Label     string
	Scope     string
	Enabled   bool
	CreatedAt time.Time
}

type UsageDelta struct {
	KeyID              string
	BytesSentDelta     int64
	BytesReceivedDelta int64
}

type UsageDay struct {
	Day            string // "2006-01-02"
	BytesSent      int64
	BytesReceived  int64
	ActiveKeyCount int // distinct keys that used traffic that day
}
