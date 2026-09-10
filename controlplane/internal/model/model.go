package model

import "time"

type Node struct {
	ID              string
	Name            string
	MaxKeys         int
	Status          string
	LastHeartbeatAt *time.Time
	CreatedAt       time.Time
}

type Key struct {
	ID                 string
	Label              string
	Transport          string
	DocURL             string
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

// Status reports the client-facing state of a key, folding in traffic-limit
// and expiry checks that "enabled" alone doesn't capture.
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
