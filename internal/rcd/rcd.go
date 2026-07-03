package rcd

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"

	"github.com/virinci/broadauth/internal/broadcast"
	contracts "github.com/virinci/broadauth/internal/contract"
	"github.com/virinci/broadauth/internal/message"
	"github.com/virinci/broadauth/internal/slot"
	"github.com/virinci/broadauth/pkg/bloom"
	"github.com/virinci/broadauth/pkg/hashchain"
)

type Mode int

const (
	ModeDeterministic Mode = iota
	ModeProbabilistic
	ModeAdaptive
	ModeProbAdaptive
)

const (
	// --- Congestion controller tuning (Prob-Adaptive Inf-TESLA++, §IV) ---

	// ingestQueueCap is Q_cap in D_i = Q_len / Q_cap. Q_len is the ingest
	// (pre-flush) backlog of unauthenticated packets — the place where genuine
	// backpressure accumulates when the radio/disclosure pipeline cannot keep
	// up. (F4: this replaces the old magic 10.0 whose comment wrongly said 100.
	// The real per-device value should be calibrated; see the tuner, F25.)
	ingestQueueCap = 10.0

	// latencyBaselineMs is L_base in B_i = min(1, L_avg / L_base). Single source
	// of truth — matches the paper's 20 ms (F3 removes the divergent 50 ms path).
	latencyBaselineMs = 20.0

	// Queue-dominant weighting (W_disc + W_lat = 1), matching paper Eq. 4 (F1):
	// unauthenticated-key loss is unrecoverable, broadcast latency is recoverable
	// via receiver buffering.
	wDisc = 0.70
	wLat  = 0.30

	// Multiplicative time-scaling thresholds on the Congestion Score C_i.
	scaleUpThreshold   = 0.75
	scaleDownThreshold = 0.25

	// latencyEWMAAlpha weights the most recent broadcast-latency sample so B_i
	// can decay when pressure subsides. A cumulative mean cannot decay and pins
	// B_i high for the rest of the run (F6).
	latencyEWMAAlpha = 0.2
)

// Metrics holds atomic counters for benchmarking
type Metrics struct {
	BytesSent        uint64
	BytesReceived    uint64
	MessagesSent     uint64
	MessagesReceived uint64
	OverheadBytes    uint64 // Bytes used for HMACs, Keys, BloomFilters (non-payload)
	DisclosureDrops  uint64 // F12: disclosure-queue overflow events (keys never disclosed)
	BroadcastQueueDrops uint64 // Bug-C fix: broadcast worker queue full → message dropped

	// Timing Metrics (Cumulative Nanoseconds)
	HMACDuration      int64
	HMACCount         int64
	VerifyDuration    int64
	VerifyCount       int64
	BroadcastDuration int64
	BroadcastCount    int64
}

type RCD struct {
	id        uuid.UUID
	ownerAddr string
	ethClient *ethclient.Client
	contract  *contracts.Contract

	hashChain       *hashchain.Linear
	hashchainLen    int
	disclosureDelay uint64
	simulationTime  time.Duration
	startTime       time.Time
	messageCounter  uint64

	broadcaster broadcast.Broadcaster
	receiver    broadcast.Receiver
	slotSource  slot.SlotSource

	// controlQueue and dataQueue are priority lanes for the broadcast worker
	// (Bug-X fix). Originally there was a single FIFO `broadcastQueue` that
	// trafficLoop flooded at 50 Hz; HMACs and key disclosures queued behind
	// thousands of data msgs and never reached the radio. Two queues + the
	// worker always preferring controlQueue guarantees that authentication-
	// critical traffic (HMAC, key disclosure) gets through whenever the radio
	// has bandwidth. Data msgs only use the spare bandwidth that remains.
	controlQueue chan broadcastJob
	dataQueue    chan broadcastJob

	disclosureMessages chan DisclosurePayload
	receivedHMACs      sync.Map
	commitmentKeys     sync.Map

	unverifiedMsgs sync.Map

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cachedKey     [32]byte
	cachedKeySlot uint64

	mode               Mode
	messageBuffer      [][]byte
	bufferMutex        sync.Mutex
	chainMutex         sync.Mutex
	enableBenchmarking bool

	adaptiveT      uint64
	adaptiveOffset uint64
	tMin           uint64
	tMax           uint64

	// F6: recency-weighted broadcast-latency tracker (ms) feeding B_i.
	latencyEWMAms   float64
	latencyEWMAInit bool
	latencyMu       sync.Mutex

	metrics Metrics
}

type DisclosurePayload struct {
	Message    []byte
	Key        [32]byte
	TargetSlot uint64
}

// broadcastJob is a unit of work for broadcastWorker. Either preBuilt is a
// fully-marshalled message (HMAC, key disclosure, deterministic data) ready
// to send as-is, OR preBuilt is nil and the worker assembles a fresh
// adaptive-mode Data message at dequeue time using rawData + the worker's
// captured currentSlot. The latter path is the Bug-B fix: sched.Index now
// reflects when the packet actually goes on the wire, not when traffic was
// generated 9+ seconds earlier behind the throttled broadcaster.
//
// priority distinguishes control traffic (HMAC, key disclosure) from data
// traffic. The worker drains controlQueue strictly before dataQueue so
// authentication-critical messages aren't starved by the high-rate
// trafficLoop (Bug-X fix).
type broadcastJob struct {
	preBuilt []byte
	rawData  []byte
	mode     Mode
}

type Config struct {
	UUID               uuid.UUID
	OwnerAddr          string
	EthURL             string
	ContractAddr       string
	HashchainLen       int
	DisclosureDelay    uint64
	SimulationTime     time.Duration
	Mode               Mode
	EnableBenchmarking bool
	TMin               uint64
	TMax               uint64
}

type Schedule struct {
	Index    uint64
	Duration uint64
	Offset   uint64
}

func New(cfg Config) (*RCD, error) {
	client, err := ethclient.Dial(cfg.EthURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %v", err)
	}

	contractAddress := common.HexToAddress(cfg.ContractAddr)
	contract, err := contracts.NewContract(contractAddress, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to instantiate contract: %v", err)
	}

	broadcaster, err := broadcast.NewUDPBroadcaster(broadcast.DefaultUDPConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create broadcaster: %v", err)
	}

	receiver, err := broadcast.NewUDPReceiver(broadcast.DefaultUDPConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver: %v", err)
	}

	if cfg.HashchainLen <= 0 || cfg.HashchainLen > 10*1024 {
		return nil, fmt.Errorf("invalid hashchain length: must be between 1 and %d", 10*1024)
	}

	var slotSource slot.SlotSource

	if cfg.Mode == ModeAdaptive || cfg.Mode == ModeProbAdaptive {
		slotSource = slot.NewAdaptiveSlotSource(cfg.TMin)
	} else {
		slotSource, err = slot.NewBeaconChainSlotSource(client, 3*time.Second)
		if err != nil {
			return nil, fmt.Errorf("failed to create slot source: %v", err)
		}
	}

	return &RCD{
		id:                 cfg.UUID,
		ownerAddr:          cfg.OwnerAddr,
		ethClient:          client,
		contract:           contract,
		hashchainLen:       cfg.HashchainLen,
		disclosureDelay:    cfg.DisclosureDelay,
		simulationTime:     cfg.SimulationTime,
		broadcaster:        broadcaster,
		receiver:           receiver,
		slotSource:         slotSource,
		disclosureMessages: make(chan DisclosurePayload, 1024),
		// controlQueue holds HMACs and key disclosures (small, bounded number
		// per slot — generous capacity so nothing important is ever dropped).
		// dataQueue holds the high-rate trafficLoop output; capacity is large
		// but bounded, so under sustained overload trafficLoop drops cleanly.
		controlQueue: make(chan broadcastJob, 1024),
		dataQueue:    make(chan broadcastJob, 4096),
		messageBuffer:      make([][]byte, 0),
		mode:               cfg.Mode,
		tMin:               cfg.TMin,
		tMax:               cfg.TMax,
		adaptiveT:          cfg.TMin,
		enableBenchmarking: cfg.EnableBenchmarking,
	}, nil
}

func (r *RCD) Start() error {
	r.ctx, r.cancel = context.WithTimeout(context.Background(), r.simulationTime)

	r.receiver.SetMessageHandler(r.handleMessage)
	if err := r.receiver.Start(r.ctx); err != nil {
		return fmt.Errorf("failed to start receiver: %v", err)
	}

	// Pre-fetch the hashchain SYNCHRONOUSLY before launching any worker.
	// CurrentSlotKey acquires chainMutex AND calls RequestHashChain on a cold
	// start. The owner's storeAdaptiveKey on-chain transaction takes ~12 s on
	// anvil (`-block-time 12`), so on the very first call this blocks the
	// caller for ~12 s. If we let the broadcastWorker make that first call
	// after Start() returns, the worker holds chainMutex for those 12 s and
	// every flushAdaptiveBatch goroutine queues behind it — which is exactly
	// what the previous sweep showed (slot 1's FLUSH-SUCCESS log only fired
	// 17 s after slot 1 was initiated). By blocking here, the 12 s happens
	// once, deterministically, before any goroutine that depends on the
	// chain ever starts. The sweep harness must wait long enough between
	// rcd_proc.Popen() and applying network shaping to cover this — bumped
	// to 15 s in sweep_benchmark.py.
	if _, _, err := r.CurrentSlotKey(); err != nil {
		return fmt.Errorf("failed to pre-fetch initial hashchain: %v", err)
	}

	r.startTime = time.Now()

	// F8: traffic generation and slot control run in separate goroutines.
	// The throttled broadcaster blocks for ~len(msg)/50 seconds per send (see
	// internal/broadcast/udp_broadcast.go), so leaving them in the same select
	// stalls slot ticks, congestion sampling, and batch flushes behind every
	// traffic broadcast. Splitting them lets the controller sample state and
	// emit batches on schedule regardless of radio backpressure.
	//
	// Bug-C: an additional broadcastWorker drains all four broadcast sites
	// (trafficLoop data msgs, slotLoop flush HMACs, disclosureWorker key
	// disclosures, deterministic-mode HMACs) so producers never block on
	// b.mu/Sleep. messageBuffer fills at trafficLoop rate (50 Hz), so D_i
	// finally has a non-trivial signal to react to.
	if r.enableBenchmarking {
		r.wg.Add(6)
		go r.benchmarkWorker()
	} else {
		log.SetOutput(os.Stderr)
		r.wg.Add(5)
	}

	go r.trafficLoop()
	go r.slotLoop()
	go r.disclosureWorker()
	go r.cleanupWorker()
	go r.broadcastWorker()

	modeStr := "DETERMINISTIC"
	switch r.mode {
	case ModeProbabilistic:
		modeStr = "PROBABILISTIC"
	case ModeAdaptive:
		modeStr = "ADAPTIVE"
	case ModeProbAdaptive:
		modeStr = "PROB-ADAPTIVE"
	}

	log.Printf("Starting RCD in %s mode", modeStr)

	return nil
}

func (r *RCD) Stop() error {
	r.cancel()
	r.wg.Wait()
	r.ethClient.Close()
	return nil
}

func (r *RCD) benchmarkWorker() {
	defer r.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var prevBytesSent, prevBytesRecv, prevMsgSent uint64
	var prevHMACDur, prevHMACCount int64
	var prevVerifyDur, prevVerifyCount int64
	var prevBroadcastDur, prevBroadcastCount int64

	startTime := time.Now()

	fmt.Println("   Time |      Tx Rate      |      Rx Rate      |  Msg/s  | Mem(MB) | Total Overhead | Avg HMAC (µs) | Avg Verify (µs) | Avg Bcast (µs)")
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------")

	for {
		select {
		case <-r.ctx.Done():
			return
		case t := <-ticker.C:
			currBytesSent := atomic.LoadUint64(&r.metrics.BytesSent)
			currBytesRecv := atomic.LoadUint64(&r.metrics.BytesReceived)
			currMsgSent := atomic.LoadUint64(&r.metrics.MessagesSent)
			overhead := atomic.LoadUint64(&r.metrics.OverheadBytes)

			currHMACDur := atomic.LoadInt64(&r.metrics.HMACDuration)
			currHMACCount := atomic.LoadInt64(&r.metrics.HMACCount)
			currVerifyDur := atomic.LoadInt64(&r.metrics.VerifyDuration)
			currVerifyCount := atomic.LoadInt64(&r.metrics.VerifyCount)
			currBroadcastDur := atomic.LoadInt64(&r.metrics.BroadcastDuration)
			currBroadcastCount := atomic.LoadInt64(&r.metrics.BroadcastCount)

			txRate := float64(currBytesSent-prevBytesSent) / 5.0
			rxRate := float64(currBytesRecv-prevBytesRecv) / 5.0
			msgRate := float64(currMsgSent-prevMsgSent) / 5.0

			avgHMAC := 0.0
			if diff := currHMACCount - prevHMACCount; diff > 0 {
				avgHMAC = float64(currHMACDur-prevHMACDur) / float64(diff) / 1000.0
			}

			avgVerify := 0.0
			if diff := currVerifyCount - prevVerifyCount; diff > 0 {
				avgVerify = float64(currVerifyDur-prevVerifyDur) / float64(diff) / 1000.0
			}

			avgBroadcast := 0.0
			if diff := currBroadcastCount - prevBroadcastCount; diff > 0 {
				avgBroadcast = float64(currBroadcastDur-prevBroadcastDur) / float64(diff) / 1000.0
			}

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memUsage := float64(m.Alloc) / 1024 / 1024

			elapsed := t.Sub(startTime).Seconds()

			fmt.Printf("%7.1fs | %10.2f B/s | %10.2f B/s | %7.1f | %7.2f | %12d B | %12.2f | %14.2f | %13.2f\n",
				elapsed, txRate, rxRate, msgRate, memUsage, overhead, avgHMAC, avgVerify, avgBroadcast)

			prevBytesSent = currBytesSent
			prevBytesRecv = currBytesRecv
			prevMsgSent = currMsgSent
			prevHMACDur = currHMACDur
			prevHMACCount = currHMACCount
			prevVerifyDur = currVerifyDur
			prevVerifyCount = currVerifyCount
			prevBroadcastDur = currBroadcastDur
			prevBroadcastCount = currBroadcastCount
		}
	}
}

func (r *RCD) RequestHashChain(currentSlot uint64) error {
	const maxRetries = 3
	var lastErr error
	for i := range maxRetries {
		if err := r.requestHashChainOnce(currentSlot); err != nil {
			lastErr = err
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to request hashchain after retries: %v", lastErr)
}

func (r *RCD) requestHashChainOnce(currentSlot uint64) error {
	conn, err := net.Dial("tcp", r.ownerAddr)
	if err != nil {
		return fmt.Errorf("failed to dial owner: %v", err)
	}
	defer conn.Close()

	payload := make([]byte, 48)
	copy(payload[:16], r.id[:])
	new(big.Int).SetUint64(currentSlot).FillBytes(payload[16:48])

	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return fmt.Errorf("failed to set deadline: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("failed to write request: %v", err)
	}

	chainSize := (r.hashchainLen + 1) * 32
	chain := make([]byte, chainSize)
	if _, err := io.ReadFull(conn, chain); err != nil {
		return fmt.Errorf("failed to read hashchain response: %v", err)
	}

	hashChain, err := hashchain.NewLinearFromExisting(sha256.New(), chain)
	if err != nil || hashChain.Remaining() < 1 {
		return fmt.Errorf("failed to initialize hashchain from bytes: %v", err)
	}

	r.hashChain = hashChain
	return nil
}

// trafficLoop generates synthetic application traffic at a fixed cadence and
// hands each message to r.broadcast(). It may block on the throttled radio,
// but that no longer interferes with slot scheduling — slotLoop runs in its
// own goroutine (F8).
//
// 100 ms cadence (10 Hz) is chosen against the broadcaster's ~50 B/s bottleneck
// (internal/broadcast/udp_broadcast.go intentionally simulates a very weak
// radio). At 50 Hz the Bloom filters per batch grew to ~270 bytes — each
// disclosure msg took >7 s of radio time and starved data msgs, leaving the
// receiver's unverifiedMsgs map empty and producing zero authentications. At
// 10 Hz batches are 5× smaller, BFs are 5× smaller, disclosures take ~2.4 s
// each, and ~20 data msgs fit through per 100 s run. D_i still saturates
// (10 msgs/slot ≥ ingestQueueCap=10), so the controller's toggle remains
// active.
func (r *RCD) trafficLoop() {
	defer r.wg.Done()
	trafficTicker := time.NewTicker(100 * time.Millisecond)
	defer trafficTicker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-trafficTicker.C:
			msg := fmt.Sprintf("%s: payload_data_%d", r.id, atomic.AddUint64(&r.messageCounter, 1))
			if err := r.broadcast([]byte(msg)); err != nil {
				log.Printf("Failed to broadcast message: %v", err)
			}
		}
	}
}

// slotLoop drives slot-boundary work: probabilistic / prob-adaptive batch
// flushes plus congestion-metric sampling and the adaptive T_i toggle. It is
// independent of trafficLoop so the slot timer can never be stalled behind a
// blocked traffic broadcast (F8).
func (r *RCD) slotLoop() {
	defer r.wg.Done()
	slotTicker := r.slotSource.Ticker(r.ctx)

	for {
		select {
		case <-r.ctx.Done():
			return
		case currentSlot := <-slotTicker:
			go func(slotToFlush uint64) {
				switch r.mode {
				case ModeProbAdaptive:
					if err := r.flushAdaptiveBatch(slotToFlush); err != nil {
						log.Printf("[PROB-ADAPTIVE: FLUSH-ERROR] Failed to flush adaptive batch for slot %d: %v", slotToFlush, err)
					}
				case ModeProbabilistic:
					if err := r.flushBatch(slotToFlush); err != nil {
						log.Printf("Failed to flush batch for slot %d: %v", slotToFlush, err)
					}
				}
			}(uint64(currentSlot) - 1)

			if r.mode == ModeAdaptive || r.mode == ModeProbAdaptive {
				// F2/F3: D_i, B_i and the score all come from one place, so the
				// logged metrics are exactly what the controller acts on.
				di, bi, score := r.calculateTimeCongestion()

				r.bufferMutex.Lock()
				nextT := r.selectDuration(score, r.adaptiveT)

				log.Printf("[PROB-ADAPTIVE: METRICS] Slot: %d | D_i (Queue): %.2f | B_i (Latency): %.2f | C_i (Score): %.2f",
					currentSlot, di, bi, score)

				if nextT != r.adaptiveT {
					log.Printf("[PROB-ADAPTIVE: TOGGLE-ACTION] Threshold breached! Slot %d | Scaling T_i: %dms -> %dms",
						currentSlot, r.adaptiveT, nextT)

					r.adaptiveT = nextT

					if adaptiveSrc, ok := r.slotSource.(*slot.AdaptiveSlotSource); ok {
						adaptiveSrc.SetDuration(nextT)
					}
				}
				r.bufferMutex.Unlock()
			}
		}
	}
}

func (r *RCD) CurrentSlotKey() (slot.Slot, []byte, error) {
	r.chainMutex.Lock()
	defer r.chainMutex.Unlock()

	for {
		if r.hashChain == nil || r.cachedKeySlot == 0 {
			log.Printf("Hashchain exhausted or missing, refilling...")
			hashchainBeginSlot, err := r.slotSource.GetSlot()
			if err != nil {
				return 0, nil, fmt.Errorf("failed to get current slot: %v", err)
			}
			if err := r.RequestHashChain(uint64(hashchainBeginSlot)); err != nil {
				return 0, nil, fmt.Errorf("failed to request new hashchain: %v", err)
			}
			key := r.hashChain.Next()
			copy(r.cachedKey[:], key)
			r.cachedKeySlot = hashchainBeginSlot
		}

		currentSlot, err := r.slotSource.GetSlot()
		if err != nil {
			return 0, nil, fmt.Errorf("failed to get current slot in loop: %v", err)
		}

		for r.cachedKeySlot < currentSlot {
			if r.hashChain.Remaining() == 0 {
				r.cachedKeySlot = 0
				break
			}
			key := r.hashChain.Next()
			copy(r.cachedKey[:], key)
			r.cachedKeySlot++
		}

		if r.cachedKeySlot != 0 {
			return r.cachedKeySlot, r.cachedKey[:], nil
		}
	}
}

func (r *RCD) broadcast(data []byte) error {
	// Probabilistic and prob-adaptive modes: append to messageBuffer FIRST so
	// the controller's D_i reflects real load, then enqueue the raw data for
	// the worker (which captures sched.Index at send time). trafficLoop never
	// blocks on the radio — that's what Bug C was about.
	if r.mode == ModeProbabilistic || r.mode == ModeProbAdaptive {
		r.bufferMutex.Lock()
		r.messageBuffer = append(r.messageBuffer, data)
		bufLen := len(r.messageBuffer)
		r.bufferMutex.Unlock()
		if r.mode == ModeProbAdaptive {
			log.Printf("[PROB-ADAPTIVE: INGEST] Buffered packet. Current batch size: %d", bufLen)
		}

		if r.enableBenchmarking {
			atomic.AddUint64(&r.metrics.MessagesSent, 1)
		}

		// Enqueue raw data on the data lane; worker will marshal with a fresh
		// slot at send (Bug-B) and only transmit after controlQueue is drained
		// (Bug-X). Dropping a data msg here is expected under sustained
		// overload — the BF still contains it (already in messageBuffer).
		if err := r.enqueueDataBroadcast(broadcastJob{rawData: data, mode: r.mode}); err != nil {
			return err
		}
		return nil
	}

	// Deterministic & adaptive modes still need HMAC computed at generation
	// time (so it covers the same sched.Index the receiver sees), so we
	// build the bytes here and enqueue them as preBuilt.
	currentSlot, key, err := r.CurrentSlotKey()
	if err != nil {
		return fmt.Errorf("failed to get current slot/key: %v", err)
	}

	var payloadToBroadcast []byte
	var sched Schedule

	if r.mode == ModeAdaptive {
		r.bufferMutex.Lock()
		sched = Schedule{
			Index:    currentSlot,
			Duration: r.adaptiveT,
			Offset:   r.adaptiveOffset,
		}
		r.adaptiveOffset += r.adaptiveT
		r.bufferMutex.Unlock()
		payloadToBroadcast = packAdaptiveData(sched, data)
	} else {
		payloadToBroadcast = data
	}

	dataMsg := message.NewMessage(r.id, currentSlot, message.MessageKindData, payloadToBroadcast)
	msgBytes, err := dataMsg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal data message: %v", err)
	}

	if r.enableBenchmarking {
		atomic.AddUint64(&r.metrics.BytesSent, uint64(len(msgBytes)))
		atomic.AddUint64(&r.metrics.MessagesSent, 1)
	}

	// Deterministic/Adaptive: the per-msg Data message is application data
	// (data lane). The per-msg HMAC that follows below goes on control.
	if err := r.enqueueDataBroadcast(broadcastJob{preBuilt: msgBytes}); err != nil {
		return err
	}

	switch r.mode {
	case ModeDeterministic:
		return r.broadcastDeterministic(data, key, currentSlot)
	case ModeAdaptive:
		return r.broadcastAdaptive(payloadToBroadcast, key, currentSlot, sched)
	default:
		return fmt.Errorf("unknown mode")
	}
}

func (r *RCD) broadcastDeterministic(data []byte, key []byte, currentSlot uint64) error {
	startHMAC := time.Now()
	signature := r.calculateHMAC(key, data)
	if r.enableBenchmarking {
		atomic.AddInt64(&r.metrics.HMACDuration, time.Since(startHMAC).Nanoseconds())
		atomic.AddInt64(&r.metrics.HMACCount, 1)
	}

	hmacMessage := message.NewMessage(r.id, currentSlot, message.MessageKindHMAC, signature)
	hmacData, err := hmacMessage.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal HMAC message: %v", err)
	}

	if r.enableBenchmarking {
		l := uint64(len(hmacData))
		atomic.AddUint64(&r.metrics.BytesSent, l)
		atomic.AddUint64(&r.metrics.OverheadBytes, l)
	}

	if err := r.enqueueControlBroadcast(broadcastJob{preBuilt: hmacData}); err != nil {
		return fmt.Errorf("failed to enqueue HMAC message: %v", err)
	}

	log.Printf("Sent HMAC message for slot %d", currentSlot)

	targetSlot := currentSlot + r.disclosureDelay
	keyArray := [32]byte{}
	copy(keyArray[:], key)

	select {
	case r.disclosureMessages <- DisclosurePayload{
		Message:    data,
		Key:        keyArray,
		TargetSlot: targetSlot,
	}:
	default:
		atomic.AddUint64(&r.metrics.DisclosureDrops, 1)
		log.Printf("[DISCLOSURE-OVERFLOW] Disclosure queue full; key for this slot will never be broadcast")
		return fmt.Errorf("disclosure queue full")
	}
	return nil
}

// broadcastProbabilistic / broadcastProbadaptive were folded into r.broadcast's
// ModeProbabilistic / ModeProbAdaptive branch as part of the Bug-C fix — the
// buffer append now happens BEFORE the radio enqueue so the controller's D_i
// reflects load in real time.

func (r *RCD) broadcastAdaptive(packedData []byte, key []byte, currentSlot uint64, sched Schedule) error {
	startHMAC := time.Now()
	signature := r.calculateHMAC(key, packedData)
	if r.enableBenchmarking {
		atomic.AddInt64(&r.metrics.HMACDuration, time.Since(startHMAC).Nanoseconds())
		atomic.AddInt64(&r.metrics.HMACCount, 1)
	}

	hmacMessage := message.NewMessage(r.id, currentSlot, message.MessageKindHMAC, signature)
	hmacData, err := hmacMessage.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal adaptive HMAC message: %v", err)
	}

	if r.enableBenchmarking {
		l := uint64(len(hmacData))
		atomic.AddUint64(&r.metrics.BytesSent, l)
		atomic.AddUint64(&r.metrics.OverheadBytes, l)
	}

	if err := r.enqueueControlBroadcast(broadcastJob{preBuilt: hmacData}); err != nil {
		return fmt.Errorf("failed to enqueue adaptive HMAC message: %v", err)
	}

	log.Printf("Sent Adaptive HMAC message for slot %d (T_i: %dms)", currentSlot, sched.Duration)

	targetSlot := currentSlot + r.disclosureDelay
	keyArray := [32]byte{}
	copy(keyArray[:], key)

	select {
	case r.disclosureMessages <- DisclosurePayload{
		Message:    packedData,
		Key:        keyArray,
		TargetSlot: targetSlot,
	}:
	default:
		atomic.AddUint64(&r.metrics.DisclosureDrops, 1)
		log.Printf("[DISCLOSURE-OVERFLOW] Disclosure queue full; key for this slot will never be broadcast")
		return fmt.Errorf("disclosure queue full")
	}
	return nil
}

func (r *RCD) flushBatch(slot uint64) error {
	r.bufferMutex.Lock()
	if len(r.messageBuffer) == 0 {
		r.bufferMutex.Unlock()
		return nil
	}
	messages := r.messageBuffer
	count := len(messages)
	r.messageBuffer = make([][]byte, 0)
	r.bufferMutex.Unlock()

	bf := bloom.New(uint(len(messages)), 0.01)
	for _, msg := range messages {
		bf.Add(msg)
	}
	bfData := bf.Bytes()

	_, key, err := r.CurrentSlotKey()
	if err != nil {
		return fmt.Errorf("failed to get current slot/key for batch flush: %v", err)
	}

	startHMAC := time.Now()
	signature := r.calculateHMAC(key, bfData)
	if r.enableBenchmarking {
		atomic.AddInt64(&r.metrics.HMACDuration, time.Since(startHMAC).Nanoseconds())
		atomic.AddInt64(&r.metrics.HMACCount, 1)
	}

	hmacMsg := message.NewMessage(r.id, slot, message.MessageKindHMAC, signature)
	hmacBytes, err := hmacMsg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal batch HMAC message: %v", err)
	}

	if r.enableBenchmarking {
		l := uint64(len(hmacBytes))
		atomic.AddUint64(&r.metrics.BytesSent, l)
		atomic.AddUint64(&r.metrics.OverheadBytes, l)
	}

	if err := r.enqueueControlBroadcast(broadcastJob{preBuilt: hmacBytes}); err != nil {
		return fmt.Errorf("failed to enqueue batch HMAC message: %v", err)
	}

	log.Printf("Sent HMAC message (Batch of %d messages) for slot %d", count, slot)

	targetSlot := slot + r.disclosureDelay
	keyArray := [32]byte{}
	copy(keyArray[:], key)

	select {
	case r.disclosureMessages <- DisclosurePayload{
		Message:    bfData,
		Key:        keyArray,
		TargetSlot: targetSlot,
	}:
	default:
		atomic.AddUint64(&r.metrics.DisclosureDrops, 1)
		log.Printf("[DISCLOSURE-OVERFLOW] Disclosure queue full; key for this slot will never be broadcast")
		return fmt.Errorf("disclosure queue full")
	}
	return nil
}

func (r *RCD) flushAdaptiveBatch(slot uint64) error {
	r.bufferMutex.Lock()

	log.Printf("[PROB-ADAPTIVE: FLUSH] Slot %d initiated. Extracting %d packets from buffer.", slot, len(r.messageBuffer))

	if len(r.messageBuffer) == 0 {
		r.bufferMutex.Unlock()
		log.Printf("[PROB-ADAPTIVE: FLUSH] Slot %d empty. Skipping broadcast.", slot)
		return nil
	}
	messages := r.messageBuffer
	count := len(messages)
	r.messageBuffer = make([][]byte, 0)

	sched := Schedule{
		Index:    slot,
		Duration: r.adaptiveT,
		Offset:   r.adaptiveOffset,
	}
	r.adaptiveOffset += r.adaptiveT
	r.bufferMutex.Unlock()

	bf := bloom.New(uint(len(messages)), 0.01)
	for _, msg := range messages {
		bf.Add(msg)
	}
	bfData := bf.Bytes()

	packedData := packAdaptiveData(sched, bfData)

	log.Printf("[PROB-ADAPTIVE: FLUSH] Created Bloom Filter. Compressed %d packets into %d bytes.", count, len(packedData))

	_, key, err := r.CurrentSlotKey()
	if err != nil {
		log.Printf("[PROB-ADAPTIVE: FLUSH-ERROR] Failed to get key for slot %d: %v", slot, err)
		return fmt.Errorf("failed to get current slot/key for adaptive batch flush: %v", err)
	}

	log.Printf("[PROB-ADAPTIVE: FLUSH] Acquired Key for Slot %d. Proceeding to HMAC & Broadcast.", slot)

	startHMAC := time.Now()
	signature := r.calculateHMAC(key, packedData)
	if r.enableBenchmarking {
		atomic.AddInt64(&r.metrics.HMACDuration, time.Since(startHMAC).Nanoseconds())
		atomic.AddInt64(&r.metrics.HMACCount, 1)
	}

	hmacMsg := message.NewMessage(r.id, slot, message.MessageKindHMAC, signature)
	hmacBytes, err := hmacMsg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal adaptive batch HMAC: %v", err)
	}

	// --- CRITICAL FIX: QUEUE THE KEY FIRST ---
	// Drop the key into the queue before getting stuck at the hardware interface.
	targetSlot := slot + r.disclosureDelay
	keyArray := [32]byte{}
	copy(keyArray[:], key)

	select {
	case r.disclosureMessages <- DisclosurePayload{
		Message:    packedData,
		Key:        keyArray,
		TargetSlot: targetSlot,
	}:
	default:
		atomic.AddUint64(&r.metrics.DisclosureDrops, 1)
		log.Printf("[DISCLOSURE-OVERFLOW] Disclosure queue full; key for this slot will never be broadcast")
		return fmt.Errorf("disclosure queue full")
	}

	// --- ENQUEUE HMAC (worker handles the slow radio Sleep — Bug C fix) ---
	if r.enableBenchmarking {
		l := uint64(len(hmacBytes))
		atomic.AddUint64(&r.metrics.BytesSent, l)
		atomic.AddUint64(&r.metrics.OverheadBytes, l)
	}

	if err := r.enqueueControlBroadcast(broadcastJob{preBuilt: hmacBytes}); err != nil {
		log.Printf("[PROB-ADAPTIVE: FLUSH-ERROR] Enqueue failed on slot %d: %v", slot, err)
		return fmt.Errorf("failed to enqueue adaptive batch HMAC: %v", err)
	}

	log.Printf("[PROB-ADAPTIVE: FLUSH-SUCCESS] Batch of %d packets for slot %d broadcasted successfully.", count, slot)

	return nil
}

func (r *RCD) disclosureWorker() {
	defer r.wg.Done()
	// F8 / slot-source hygiene: poll GetSlot() instead of spawning a second
	// Ticker. AdaptiveSlotSource.Ticker() owns the slot-advance goroutine, so
	// every extra Ticker() call advances currentSlot at an additional 1/T rate
	// — effectively halving disclosureDelay's wall-time and forcing legitimate
	// packets past the security cutoff. Polling decouples this worker's
	// wakeups from slot advancement.
	pollTicker := time.NewTicker(100 * time.Millisecond)
	defer pollTicker.Stop()
	pendingDisclosures := make([]DisclosurePayload, 0)

	for {
		select {
		case <-pollTicker.C:
			cs, err := r.slotSource.GetSlot()
			if err != nil {
				continue
			}
			currentSlot := uint64(cs)
			for {
				select {
				case disclosure := <-r.disclosureMessages:
					pendingDisclosures = append(pendingDisclosures, disclosure)
				default:
					goto ProcessPending
				}
			}
		ProcessPending:
			// Bug-Y fix: iterate ALL pending disclosures and send the ones
			// whose TargetSlot has been reached. The previous prefix-based
			// scan (`break` on first not-ready) silently dropped every
			// ready disclosure that came *after* a not-ready one in the
			// slice — which happens routinely because flushAdaptiveBatch
			// runs in a spawned goroutine per slot and completes out of
			// order, so pendingDisclosures is NOT sorted by TargetSlot.
			stillPending := pendingDisclosures[:0]
			for _, disclosure := range pendingDisclosures {
				if disclosure.TargetSlot > currentSlot {
					stillPending = append(stillPending, disclosure)
					continue
				}
				disclosureMsg := message.NewMessage(
					r.id,
					disclosure.TargetSlot,
					message.MessageKindKeyMessage,
					append(disclosure.Key[:], disclosure.Message...),
				)
				data, err := disclosureMsg.Marshal()
				if err != nil {
					log.Printf("Failed to marshal disclosure: %v", err)
					continue
				}
				if r.enableBenchmarking {
					l := uint64(len(data))
					atomic.AddUint64(&r.metrics.BytesSent, l)
					atomic.AddUint64(&r.metrics.OverheadBytes, l)
				}
				if err := r.enqueueControlBroadcast(broadcastJob{preBuilt: data}); err != nil {
					log.Printf("Failed to enqueue disclosure: %v", err)
					// Keep this disclosure pending; control queue may have
					// space on the next tick.
					stillPending = append(stillPending, disclosure)
					continue
				}
				log.Printf("Sent key disclosure message for slot %d", disclosure.TargetSlot)
			}
			pendingDisclosures = stillPending
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *RCD) cleanupWorker() {
	defer r.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			currentSlot, err := r.slotSource.GetSlot()
			if err != nil {
				log.Printf("Failed to get slot for cleanup: %v", err)
				continue
			}
			retentionThreshold := r.disclosureDelay + 20
			r.unverifiedMsgs.Range(func(key, value interface{}) bool {
				if key.(uint64)+retentionThreshold < currentSlot {
					r.unverifiedMsgs.Delete(key)
				}
				return true
			})
		}
	}
}

func (r *RCD) handleMessage(data []byte) {
	if r.enableBenchmarking {
		atomic.AddUint64(&r.metrics.BytesReceived, uint64(len(data)))
		atomic.AddUint64(&r.metrics.MessagesReceived, 1)
	}

	receivedMessage := &message.Message{}
	if err := receivedMessage.Unmarshal(data); err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return
	}

	currentSlot, err := r.slotSource.GetSlot()
	if err != nil {
		log.Printf("Failed to get current slot in handler: %v", err)
		return
	}

	if receivedMessage.Kind == message.MessageKindData {
		if r.mode == ModeAdaptive || r.mode == ModeProbAdaptive {
			sched, rawData, err := unpackAdaptiveData(receivedMessage.Data)
			if err != nil {
				log.Printf("Failed to unpack adaptive data: %v", err)
				return
			}

			securityCutoff := sched.Index + r.disclosureDelay
			if currentSlot >= securityCutoff {
				log.Printf("[SECURITY] Dropped adaptive message for slot %d (arrived too late)", sched.Index)
				return
			}

			r.storeUnverifiedMessage(sched.Index, rawData)
			return
		}

		securityCutoff := receivedMessage.Slot + r.disclosureDelay
		if currentSlot >= securityCutoff {
			log.Printf("[SECURITY] Dropped message for slot %d (arrived too late)", receivedMessage.Slot)
			return
		}
		r.storeUnverifiedMessage(receivedMessage.Slot, receivedMessage.Data)
		return
	}

	if receivedMessage.Kind == message.MessageKindKeyMessage {
		log.Printf("Received key disclosure message from %s for slot %d", receivedMessage.SenderID, receivedMessage.Slot)

		key, payload := receivedMessage.Data[:32], receivedMessage.Data[32:]

		var hmacArray [32]byte
		startHMAC := time.Now()
		copy(hmacArray[:], r.calculateHMAC(key, payload))
		if r.enableBenchmarking {
			atomic.AddInt64(&r.metrics.HMACDuration, time.Since(startHMAC).Nanoseconds())
			atomic.AddInt64(&r.metrics.HMACCount, 1)
		}

		if _, exists := r.receivedHMACs.LoadAndDelete(hmacArray); !exists {
			log.Printf("No matching HMAC found for key disclosure from %s", receivedMessage.SenderID)
			return
		}

		startVerify := time.Now()
		verified := r.verifyKey(receivedMessage.SenderID, key)
		if r.enableBenchmarking {
			atomic.AddInt64(&r.metrics.VerifyDuration, time.Since(startVerify).Nanoseconds())
			atomic.AddInt64(&r.metrics.VerifyCount, 1)
		}

		if !verified {
			log.Printf("Failed to verify key from %s", receivedMessage.SenderID)
			return
		}

		switch r.mode {
		case ModeProbabilistic:
			bf := bloom.FromBytes(payload)
			if bf != nil {
				targetSlot := receivedMessage.Slot - r.disclosureDelay
				// Bug-A fix: data messages can be stored under sched.Index
				// values arbitrarily far from this batch's flush slot — there
				// is no slot-arithmetic relationship between when a data msg
				// was generated and when its batch's flush happened, only a
				// content (BF membership) relationship. Iterate ALL pending
				// unverified messages; the BF itself decides membership.
				var msgs [][]byte
				r.unverifiedMsgs.Range(func(_, v interface{}) bool {
					msgs = append(msgs, v.([][]byte)...)
					return true
				})

				verifiedCount := 0
				for _, m := range msgs {
					if bf.Check(m) {
						log.Printf("[SUCCESS] Verified message via Bloom Filter: %s", string(m))
						verifiedCount++
					}
				}
				if verifiedCount > 0 {
					log.Printf("[SUCCESS] Probabilistic Batch Verification: %d messages authenticated for slot %d", verifiedCount, targetSlot)
				} else {
					log.Printf("[BATCH-EMPTY] Probabilistic Batch Verification: 0 messages authenticated for slot %d (Bloom filter matched none of the buffered messages)", targetSlot)
				}
				// Don't bulk-delete: msgs not in this BF may match a later
				// batch's BF. cleanupWorker evicts after disclosureDelay+20.
			}
		case ModeAdaptive:
			sched, actualData, err := unpackAdaptiveData(payload)
			if err != nil {
				log.Printf("Failed to unpack verified adaptive payload: %v", err)
				return
			}
			log.Printf("[SUCCESS] Verified adaptive message from %s: %s (Slot %d, T_i %dms)", receivedMessage.SenderID, string(actualData), sched.Index, sched.Duration)
			r.unverifiedMsgs.Delete(sched.Index)
		case ModeProbAdaptive:
			sched, bfPayload, err := unpackAdaptiveData(payload)
			if err != nil {
				log.Printf("Failed to unpack prob-adaptive payload: %v", err)
				return
			}
			bf := bloom.FromBytes(bfPayload)
			if bf != nil {
				targetSlot := receivedMessage.Slot - r.disclosureDelay

				// Bug-A fix: iterate ALL pending unverified messages. Even
				// the prior ±1 "jitter-resistant" window assumed that
				// sched.Index ≈ flush slot, but with the broadcast worker
				// queue (Bug-C fix) sched.Index can lag the flush slot by
				// many slots. Membership is determined by the BF, not by
				// slot arithmetic.
				var msgs [][]byte
				r.unverifiedMsgs.Range(func(_, v interface{}) bool {
					msgs = append(msgs, v.([][]byte)...)
					return true
				})

				verifiedCount := 0
				for _, m := range msgs {
					if bf.Check(m) {
						verifiedCount++
					}
				}
				if verifiedCount > 0 {
					log.Printf("[SUCCESS] Prob-Adaptive Batch Verification: %d messages authenticated for slot %d (T_i: %dms)", verifiedCount, targetSlot, sched.Duration)
				} else {
					log.Printf("[BATCH-EMPTY] Prob-Adaptive Batch Verification: 0 messages authenticated for slot %d (T_i: %dms)", targetSlot, sched.Duration)
				}
				// cleanupWorker evicts unverified msgs after retention threshold.
			}
		default:
			log.Printf("[SUCCESS] Verified message from %s: %s", receivedMessage.SenderID, string(payload))
		}
	} else {
		log.Printf("Received HMAC message from %s for slot %d", receivedMessage.SenderID, receivedMessage.Slot)
		var hmacArray [32]byte
		copy(hmacArray[:], receivedMessage.Data)
		r.receivedHMACs.Store(hmacArray, true)
	}
}

func (r *RCD) storeUnverifiedMessage(slot uint64, data []byte) {
	value, _ := r.unverifiedMsgs.LoadOrStore(slot, make([][]byte, 0))
	msgs := value.([][]byte)
	msgs = append(msgs, data)
	r.unverifiedMsgs.Store(slot, msgs)
}

func packAdaptiveData(sched Schedule, data []byte) []byte {
	buf := make([]byte, 24+len(data))
	binary.BigEndian.PutUint64(buf[0:8], sched.Index)
	binary.BigEndian.PutUint64(buf[8:16], sched.Duration)
	binary.BigEndian.PutUint64(buf[16:24], sched.Offset)
	copy(buf[24:], data)
	return buf
}

func unpackAdaptiveData(payload []byte) (Schedule, []byte, error) {
	if len(payload) < 24 {
		return Schedule{}, nil, fmt.Errorf("payload too short for adaptive metadata")
	}
	sched := Schedule{
		Index:    binary.BigEndian.Uint64(payload[0:8]),
		Duration: binary.BigEndian.Uint64(payload[8:16]),
		Offset:   binary.BigEndian.Uint64(payload[16:24]),
	}
	return sched, payload[24:], nil
}

func (r *RCD) verifyKey(senderID uuid.UUID, key []byte) bool {
	commitmentKey, ok := r.commitmentKeys.Load(senderID)
	if !ok {
		contractKey, _, _, _, err := r.contract.GetKey(&bind.CallOpts{}, new(big.Int).SetBytes(senderID[:]))
		if err != nil || len(contractKey) == 0 {
			return false
		}
		commitmentKey = []byte(contractKey)
		r.commitmentKeys.Store(senderID, commitmentKey)
	}

	currentKey := make([]byte, 32)
	copy(currentKey, key)

	if len(currentKey) == len(commitmentKey.([]byte)) && hmac.Equal(currentKey, commitmentKey.([]byte)) {
		r.commitmentKeys.Store(senderID, currentKey)
		return true
	}

	for range r.hashchainLen + 1 {
		h := sha256.New()
		h.Write(currentKey)
		currentKey = h.Sum(nil)
		if len(currentKey) == len(commitmentKey.([]byte)) && hmac.Equal(currentKey, commitmentKey.([]byte)) {
			r.commitmentKeys.Store(senderID, key)
			return true
		}
	}
	return false
}

func (r *RCD) calculateHMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// calculateTimeCongestion returns the disclosure-queue pressure D_i, the
// broadcast-latency penalty B_i, and the combined Congestion Score C_i. All
// three come from here so the logged metrics are exactly the values the
// controller acts on (F2/F3).
//
// D_i is measured on the ingest (pre-flush) buffer: that is where genuine
// backpressure accumulates as unauthenticated packets when the radio/disclosure
// pipeline cannot keep up. NOTE: paper Eq. 2 must describe D_i as this ingest
// backlog, not "pending disclosure events" (see docs/paper-corrections.md).
//
// B_i uses the recency-weighted (EWMA) latency so the signal can recover when
// pressure subsides; a cumulative mean cannot decay and would pin B_i high (F6).
func (r *RCD) calculateTimeCongestion() (di float64, bi float64, score float64) {
	r.bufferMutex.Lock()
	backlog := float64(len(r.messageBuffer))
	r.bufferMutex.Unlock()

	di = backlog / ingestQueueCap
	if di > 1.0 {
		di = 1.0
	}

	bi = r.broadcastLatencyEWMAms() / latencyBaselineMs
	if bi > 1.0 {
		bi = 1.0
	}

	score = wDisc*di + wLat*bi
	return di, bi, score
}

// observeBroadcastLatency feeds the EWMA latency tracker used by the congestion
// controller. Called on every broadcast regardless of the benchmark flag so the
// controller always has a live, recency-weighted signal (F6).
func (r *RCD) observeBroadcastLatency(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	r.latencyMu.Lock()
	if r.latencyEWMAInit {
		r.latencyEWMAms = latencyEWMAAlpha*ms + (1-latencyEWMAAlpha)*r.latencyEWMAms
	} else {
		r.latencyEWMAms = ms
		r.latencyEWMAInit = true
	}
	r.latencyMu.Unlock()
}

func (r *RCD) broadcastLatencyEWMAms() float64 {
	r.latencyMu.Lock()
	defer r.latencyMu.Unlock()
	return r.latencyEWMAms
}

// enqueueDataBroadcast hands a high-rate data-msg job to the worker via the
// data lane. Non-blocking: if the data lane is full we drop and count it.
// trafficLoop is the sole producer here; HMACs/disclosures use the control
// lane (Bug-X fix).
func (r *RCD) enqueueDataBroadcast(job broadcastJob) error {
	select {
	case r.dataQueue <- job:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		atomic.AddUint64(&r.metrics.BroadcastQueueDrops, 1)
		return fmt.Errorf("broadcast queue full")
	}
}

// enqueueControlBroadcast hands an authentication-critical job (HMAC, key
// disclosure, deterministic-mode HMAC) to the worker via the control lane.
// The worker drains controlQueue strictly before dataQueue, so these never
// starve behind trafficLoop's flood (Bug-X fix).
func (r *RCD) enqueueControlBroadcast(job broadcastJob) error {
	select {
	case r.controlQueue <- job:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
		atomic.AddUint64(&r.metrics.BroadcastQueueDrops, 1)
		return fmt.Errorf("control queue full")
	}
}

// broadcastWorker drains the priority queues through the throttled radio.
// Each iteration prefers controlQueue (HMACs + disclosures); only when it is
// empty does the worker take from dataQueue. This is the Bug-X fix:
// authentication-critical traffic can't be starved behind trafficLoop's flood
// of data messages.
//
// For adaptive-mode Data messages (preBuilt == nil) the worker captures
// currentSlot AT DEQUEUE — the moment closest to actual wire-time we can get
// without invasive broadcaster surgery. This is the Bug-B fix: the receiver's
// "[SECURITY] arrived too late" cutoff is computed against this captured slot.
func (r *RCD) broadcastWorker() {
	defer r.wg.Done()
	for {
		// 1) Prefer control traffic: drain anything immediately available.
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.controlQueue:
			r.transmit(job)
			continue
		default:
		}

		// 2) Otherwise wait on either lane (control still preferred on tie
		//    by step 1 next iteration).
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.controlQueue:
			r.transmit(job)
		case job := <-r.dataQueue:
			r.transmit(job)
		}
	}
}

// transmit performs the actual broadcaster.Broadcast call and updates metrics.
// Factored out so broadcastWorker's queue-priority logic stays readable.
func (r *RCD) transmit(job broadcastJob) {
	data := job.preBuilt
	if data == nil {
		assembled, ok := r.buildAdaptiveDataAtSendTime(job)
		if !ok {
			return
		}
		data = assembled
	}

	start := time.Now()
	err := r.broadcaster.Broadcast(r.ctx, data)
	elapsed := time.Since(start)
	r.observeBroadcastLatency(elapsed)
	if r.enableBenchmarking {
		atomic.AddInt64(&r.metrics.BroadcastDuration, elapsed.Nanoseconds())
		atomic.AddInt64(&r.metrics.BroadcastCount, 1)
	}
	if err != nil {
		log.Printf("Broadcast worker: send failed: %v", err)
	}
}

// buildAdaptiveDataAtSendTime captures the current slot just before the radio
// actually transmits, so sched.Index reflects wire-time, not generation-time.
// The receiver's cutoff (sched.Index + disclosureDelay) is computed against
// this fresher value, which collapses the late-drop window from "9+ seconds
// of broadcaster latency" down to "single-digit milliseconds of dequeue
// overhead". Returns (bytes, true) on success.
func (r *RCD) buildAdaptiveDataAtSendTime(job broadcastJob) ([]byte, bool) {
	currentSlot, _, err := r.CurrentSlotKey()
	if err != nil {
		log.Printf("Broadcast worker: skip — no current slot/key: %v", err)
		return nil, false
	}

	var payload []byte
	if job.mode == ModeAdaptive || job.mode == ModeProbAdaptive {
		r.bufferMutex.Lock()
		sched := Schedule{
			Index:    currentSlot,
			Duration: r.adaptiveT,
			Offset:   r.adaptiveOffset,
		}
		r.adaptiveOffset += r.adaptiveT
		r.bufferMutex.Unlock()
		payload = packAdaptiveData(sched, job.rawData)
	} else {
		payload = job.rawData
	}

	msg := message.NewMessage(r.id, currentSlot, message.MessageKindData, payload)
	bytes, err := msg.Marshal()
	if err != nil {
		log.Printf("Broadcast worker: marshal failed: %v", err)
		return nil, false
	}
	return bytes, true
}

func (r *RCD) selectDuration(score float64, currentT uint64) uint64 {
	nextT := currentT

	if score > scaleUpThreshold {
		nextT = currentT * 2
	} else if score < scaleDownThreshold {
		nextT = currentT / 2
	}

	if nextT > r.tMax {
		nextT = r.tMax
	}
	if nextT < r.tMin {
		nextT = r.tMin
	}

	return nextT
}
