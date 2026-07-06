package broadcast

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// defaultRadioBytesPerSec is the auth-channel budget (F15/Option-1). The
// throttled path models a constrained RCD radio carrying only the protocol's
// auth overhead (HMAC/BF/key); application data uses the unthrottled path.
const defaultRadioBytesPerSec = 250

// UDPConfig holds configuration for UDP broadcasting
type UDPConfig struct {
	BroadcastAddr    string // Address to broadcast to (e.g., "255.255.255.255:8888")
	ListenAddr       string // Address to listen on (e.g., ":8888")
	BufferSize       int    // Size of the receive buffer
	RadioBytesPerSec int    // Throttled auth-channel rate; <=0 -> default
}

// DefaultUDPConfig returns a configuration with sensible defaults
func DefaultUDPConfig() UDPConfig {
	return UDPConfig{
		BroadcastAddr: "255.255.255.255:8888",
		ListenAddr:    ":8888",
		// BufferSize:    1024,
		// Reduced to mimic ESP32 type devices
		BufferSize:       4096,
		RadioBytesPerSec: defaultRadioBytesPerSec,
	}
}

// UDPBroadcaster implements the Broadcaster interface using UDP
type UDPBroadcaster struct {
	conn             *net.UDPConn
	addr             *net.UDPAddr
	mu               sync.Mutex
	radioBytesPerSec int // auth-channel throttle rate (bytes/sec)
}

// NewUDPBroadcaster creates a new UDP broadcaster
func NewUDPBroadcaster(config UDPConfig) (*UDPBroadcaster, error) {
	addr, err := net.ResolveUDPAddr("udp", config.BroadcastAddr)
	if err != nil {
		return nil, err
	}

	// Create a UDP socket for broadcasting
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	// Set socket options to enable broadcasting
	if err := conn.SetWriteBuffer(config.BufferSize); err != nil {
		conn.Close()
		return nil, err
	}

	rate := config.RadioBytesPerSec
	if rate <= 0 {
		rate = defaultRadioBytesPerSec
	}
	return &UDPBroadcaster{
		conn:             conn,
		addr:             addr,
		radioBytesPerSec: rate,
	}, nil
}

// Broadcast implements the throttled (auth-channel) path. It sleeps for the
// modelled transmission time at the configured radio rate, then sends — this
// is the constrained budget the disclosure pipeline competes for (F15).
func (b *UDPBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	transmissionTime := time.Duration(len(data)) * time.Second / time.Duration(b.radioBytesPerSec)
	time.Sleep(transmissionTime)

	_, err := b.conn.Write(data)
	return err
}

// BroadcastUnthrottled implements the application data plane: no rate limit.
// Data is baseline traffic, not charged against the auth budget (F15/Option-1).
func (b *UDPBroadcaster) BroadcastUnthrottled(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	_, err := b.conn.Write(data)
	return err
}

// Close implements the Broadcaster interface
func (b *UDPBroadcaster) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.conn.Close()
}

// UDPReceiver implements the Receiver interface using UDP
type UDPReceiver struct {
	conn    *net.UDPConn
	config  UDPConfig
	handler MessageHandler
	done    chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
}

// NewUDPReceiver creates a new UDP receiver
func NewUDPReceiver(config UDPConfig) (*UDPReceiver, error) {
	addr, err := net.ResolveUDPAddr("udp", config.ListenAddr)
	if err != nil {
		return nil, err
	}

	// Create a UDP socket for receiving broadcasts
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	if err := conn.SetReadBuffer(config.BufferSize); err != nil {
		conn.Close()
		return nil, err
	}

	return &UDPReceiver{
		conn:   conn,
		config: config,
		done:   make(chan struct{}),
	}, nil
}

// SetMessageHandler sets the callback for received messages
func (r *UDPReceiver) SetMessageHandler(handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handler = handler
}

// Start begins listening for broadcasts
func (r *UDPReceiver) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.handler == nil {
		r.mu.Unlock()
		return errors.New("message handler not set")
	}
	r.mu.Unlock()

	r.wg.Add(1)
	go r.receiveLoop(ctx)

	return nil
}

// receiveLoop continuously receives messages until context is canceled
func (r *UDPReceiver) receiveLoop(ctx context.Context) {
	defer r.wg.Done()

	buffer := make([]byte, r.config.BufferSize)

	// Set up a channel for read operations
	readCh := make(chan readResult)

	for {
		// Start a goroutine to read from UDP socket
		go func() {
			n, _, err := r.conn.ReadFromUDP(buffer)
			readCh <- readResult{n: n, err: err, data: buffer[:n]}
		}()

		// Wait for either context cancellation or data received
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case result := <-readCh:
			if result.err != nil {
				// Check if we're shutting down
				select {
				case <-r.done:
					return
				default:
					// Real error occurred, but we continue to read
					continue
				}
			}

			// If we got data, make a copy and call the handler
			data := make([]byte, result.n)
			copy(data, result.data)

			// Call the handler
			r.mu.Lock()
			handler := r.handler
			r.mu.Unlock()

			if handler != nil {
				go handler(data) // Call handler in a separate goroutine
			}
		}
	}
}

type readResult struct {
	n    int
	err  error
	data []byte
}

// Close implements the Receiver interface
func (r *UDPReceiver) Close() error {
	close(r.done)

	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.conn.Close()
	r.wg.Wait() // Wait for receiveLoop to exit
	return err
}
