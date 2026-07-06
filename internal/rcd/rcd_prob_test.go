package rcd

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	"github.com/virinci/broadauth/internal/message"
	"github.com/virinci/broadauth/internal/slot"
	"github.com/virinci/broadauth/pkg/hashchain"
)

type mockBroadcaster struct {
	msgs [][]byte
}

func (m *mockBroadcaster) Broadcast(ctx context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.msgs = append(m.msgs, cp)
	return nil
}

func (m *mockBroadcaster) BroadcastUnthrottled(ctx context.Context, data []byte) error {
	return m.Broadcast(ctx, data)
}

func (m *mockBroadcaster) Close() error { return nil }

type fakeSlotSource struct{ slotVal uint64 }

func (f *fakeSlotSource) GetSlot() (slot.Slot, error)                 { return slot.Slot(f.slotVal), nil }
func (f *fakeSlotSource) Ticker(ctx context.Context) <-chan slot.Slot { return make(chan slot.Slot) }

func TestFlushBatchProbabilistic(t *testing.T) {
	mb := &mockBroadcaster{}
	fs := &fakeSlotSource{slotVal: 100}

	seed := []byte("unit-test-seed-0123456789")
	hc := hashchain.NewLinear(sha256.New(), seed, 16)

	r := &RCD{
		id:                 uuid.New(),
		broadcaster:        mb,
		slotSource:         fs,
		disclosureMessages: make(chan DisclosurePayload, 10),
		controlQueue:       make(chan broadcastJob, 16),
		dataQueue:          make(chan broadcastJob, 16),
		hashChain:          hc,
		hashchainLen:       16,
		cachedKeySlot:      100,
		disclosureDelay:    2,
		mode:               ModeProbabilistic,
		messageBuffer:      make([][]byte, 0),
	}
	// enqueueBroadcast selects on r.ctx.Done(); set a context for the test.
	r.ctx, r.cancel = context.WithCancel(context.Background())
	defer r.cancel()

	// prime cached key
	key := hc.Next()
	copy(r.cachedKey[:], key)

	// Bug-C fix folded broadcastProbabilistic into r.broadcast — append direct.
	r.messageBuffer = append(r.messageBuffer, []byte("m1"))
	r.messageBuffer = append(r.messageBuffer, []byte("m2"))

	if err := r.flushBatch(100); err != nil {
		t.Fatalf("flushBatch error: %v", err)
	}

	// flushBatch now enqueues HMAC on the control lane (priority queue,
	// Bug-X fix). Verify the queued bytes are an HMAC message.
	if len(r.controlQueue) == 0 {
		t.Fatalf("expected control queue to have job, got 0")
	}
	job := <-r.controlQueue
	if job.preBuilt == nil {
		t.Fatalf("expected preBuilt HMAC bytes, got nil")
	}
	var msg message.Message
	if err := msg.Unmarshal(job.preBuilt); err != nil {
		t.Fatalf("failed to unmarshal queued message: %v", err)
	}
	if msg.Kind != message.MessageKindHMAC {
		t.Fatalf("expected HMAC message kind, got %v", msg.Kind)
	}

	select {
	case dp := <-r.disclosureMessages:
		if len(dp.Message) == 0 {
			t.Fatalf("expected disclosure payload message (BF), got empty")
		}
		if dp.TargetSlot != 102 { // slot + disclosureDelay
			t.Fatalf("unexpected target slot: %d", dp.TargetSlot)
		}
	default:
		t.Fatalf("expected a disclosure payload queued")
	}
}
