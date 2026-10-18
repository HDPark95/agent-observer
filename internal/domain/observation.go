// Package domain describes observed agent activity without provider payloads or transport details.
package domain

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// RunIdentity identifies one turn within a provider's session. None of its
// individual fields is assumed to be globally unique.
type RunIdentity struct {
	Provider  string
	SessionID string
	TurnID    string
}

// Observation contains only metadata selected for export. Zero times mean that
// the source did not establish that timestamp; they are never filled with now.
// IDs and ParentID remain local identities until an exporter hashes them.
type Observation struct {
	ID            string
	Run           RunIdentity
	ParentID      string
	Kind          string
	ToolName      string
	Outcome       string
	ObservedAt    time.Time
	StartedAt     time.Time
	EndedAt       time.Time
	TurnStartedAt time.Time
}

// Key returns a stable identifier for a turn, separate from observation IDs.
func (r RunIdentity) Key() string {
	return digest("run", r.Provider, r.SessionID, r.TurnID)
}

// TraceID is the 16-byte hexadecimal trace identity shared by this turn.
func (r RunIdentity) TraceID() string { return r.Key()[:32] }

// SpanID is the 8-byte hexadecimal identity of this turn's root span.
func (r RunIdentity) SpanID() string { return digest("root-span", r.Key())[:16] }

// RunID is a stable UUID for a destination that identifies runs with UUIDs.
func (r RunIdentity) RunID() string { return stableUUID(r.Key()) }

// Key identifies an observed activity, not its current status or delivery attempt.
func (o Observation) Key() string { return digest("observation", o.Run.Key(), o.ID) }

func (o Observation) TraceID() string { return o.Run.TraceID() }
func (o Observation) SpanID() string  { return o.Key()[:16] }
func (o Observation) RunID() string   { return stableUUID(o.Key()) }

// digest encodes a tuple before hashing, so delimiters inside source IDs cannot
// make two distinct identities collide through string concatenation.
func digest(parts ...string) string {
	encoded, _ := json.Marshal(parts) // A slice of strings always has a JSON encoding.
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func stableUUID(key string) string {
	// Name-based UUIDs use the standard OID namespace. SHA-1 is required by
	// UUID version 5 here, not used as a security or integrity primitive.
	namespace := [16]byte{0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	name := append(namespace[:], []byte("agent-observer:"+key)...)
	sum := sha1.Sum(name)
	value := sum[:16]
	value[6] = value[6]&0x0f | 0x50
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:])
}
