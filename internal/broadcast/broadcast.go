package broadcast

import (
	"context"
)

// Broadcaster defines an interface for broadcasting data
type Broadcaster interface {
	// Broadcast sends data over the throttled radio (the auth/control channel:
	// HMACs, Bloom filters, key disclosures). Rate-limited to the configured
	// radio budget — the constrained resource the protocol competes for.
	Broadcast(ctx context.Context, data []byte) error

	// BroadcastUnthrottled sends data without rate limiting (the application
	// data plane). Per the Inf-TESLA++ cost model (F15/Option-1), application
	// payload is baseline traffic sent regardless; only the protocol's auth
	// overhead is charged against the constrained radio budget.
	BroadcastUnthrottled(ctx context.Context, data []byte) error

	// Close shuts down the broadcaster and releases resources
	Close() error
}

// MessageHandler is a callback function for handling received messages
type MessageHandler func(data []byte)

// Receiver defines an interface for receiving broadcasted data
type Receiver interface {
	// SetMessageHandler sets a callback function that will be called when data is received
	SetMessageHandler(handler MessageHandler)

	// Start begins listening for broadcasts
	Start(ctx context.Context) error

	// Close shuts down the receiver and releases resources
	Close() error
}
