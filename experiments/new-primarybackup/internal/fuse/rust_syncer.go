package fuse

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// RustSyncer implements Lock/Unlock/Sync using the Rust fuselog binaries.
// It maintains a persistent connection to fuserust and forwards
// the payload to each fuselog_apply instance per sync.
type RustSyncer struct {
	CoreSocket   string   // fuserust socket path
	ApplySockets []string // one per snapshot dir
	LogTiming bool

	mu            		sync.Mutex
	coreConn      		net.Conn
	syncCount     		int
	totalDuration 		time.Duration
	totalCaptureDuration 	time.Duration
	totalApplyDuration   	time.Duration
}

func (s *RustSyncer) Lock() {
	s.mu.Lock()
}

func (s *RustSyncer) Unlock() {
	s.mu.Unlock()
}

// Close closes the persistent connection to fuserust.
func (s *RustSyncer) Close() {
	if s.coreConn != nil {
		s.coreConn.Close()
		s.coreConn = nil
	}
}

// connect ensures a persistent connection to fuserust.
func (s *RustSyncer) connect() error {
	if s.coreConn != nil {
		return nil
	}
	conn, err := net.Dial("unix", s.CoreSocket)
	if err != nil {
		return fmt.Errorf("dial fuserust socket %s: %w", s.CoreSocket, err)
	}
	s.coreConn = conn
	return nil
}

// Sync requests the stateDiff from fuserust and forwards it to all
// fuselog_apply instances.
func (s *RustSyncer) Sync() error {
	start := time.Now()

	// Step 1: ensure persistent connection to fuserust
	if err := s.connect(); err != nil {
		return err
	}

	// Step 2: send 'g' to request stateDiff (log is cleared automatically after)
	if _, err := s.coreConn.Write([]byte("g")); err != nil {
		s.coreConn = nil
		return fmt.Errorf("send 'g' to fuserust: %w", err)
	}
	//log.Printf("[fuse-rust] sent 'g' to fuselog_core")

	// Step 3: read 8-byte little-endian size
	var totalSize uint64
	if err := binary.Read(s.coreConn, binary.LittleEndian, &totalSize); err != nil {
		s.coreConn = nil
		return fmt.Errorf("read stateDiff size req: %w", err)
	}
	//log.Printf("[fuse-rust] payload size: %d bytes", totalSize)

	//log.Printf("[fuse-rust] stateDiff size-%d: %d bytes", s.syncCount, totalSize)
	if totalSize == 0 {
		return nil
	}

	// Step 4: read full payload
	payload := make([]byte, totalSize)
	if _, err := io.ReadFull(s.coreConn, payload); err != nil {
		s.coreConn = nil
		return fmt.Errorf("read stateDiff payload (%d bytes): %w", totalSize, err)
	}
	//log.Printf("[fuse-rust] payload received")

	captureElapsed := time.Since(start)

	// Step 5: forward payload to each fuselog_apply instance
	for _, sockPath := range s.ApplySockets {
		//applyStart := time.Now()
		log.Printf("[fuse-rust] sending to apply socket: %s", sockPath)
		if err := s.sendToApply(sockPath, payload); err != nil {
			return fmt.Errorf("send to fuselog_apply %s: %w", sockPath, err)
		}
		//log.Printf("[fuse-rust] apply[%d]: time=%v", i, time.Since(applyStart))
	}

	applyElapsed := time.Since(start) - captureElapsed
	totalElapsed := time.Since(start)
	//log.Printf("[fuse-rust] total: capture=%v apply=%v total=%v", captureElapsed, applyElapsed, totalElapsed)
	s.syncCount++
	s.totalDuration += totalElapsed
	s.totalCaptureDuration += captureElapsed
	s.totalApplyDuration += applyElapsed

	if s.LogTiming && s.syncCount%100 == 0 {
		 avgTotal := s.totalDuration / time.Duration(s.syncCount)
		 avgCapture := s.totalCaptureDuration / time.Duration(s.syncCount)
		 avgApply := s.totalApplyDuration / time.Duration(s.syncCount)
		 log.Printf("[fuse-rust] avg over %d requests — capture: %v | apply: %v | total: %v",
		 s.syncCount, avgCapture, avgApply, avgTotal)
	}

	return nil
}

// sendToApply connects to a fuselog_apply socket, sends the payload,
// and waits for "CONFIRMED".
func (s *RustSyncer) sendToApply(sockPath string, payload []byte) error {
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		return fmt.Errorf("dial fuselog_apply socket: %w", err)
	}
	defer conn.Close()

	if uc, ok := conn.(*net.UnixConn); ok {
		uc.SetWriteBuffer(4 * 1024 * 1024)
		uc.SetReadBuffer(1024)
	}

	writeErr := make(chan error, 1)
	//writeStart := time.Now()
	go func() {
		_, err := conn.Write(payload)
		//log.Printf("[fuse-rust] write done: %v", time.Since(writeStart))
		if err != nil {
			writeErr <- fmt.Errorf("write payload: %w", err)
			return
		}
		if tc, ok := conn.(*net.UnixConn); ok {
			tc.CloseWrite()
		}
		writeErr <- nil
	}()

	resp := make([]byte, 9)
	//log.Printf("[fuse-rust] waiting for confirmation...")
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("read confirmation: %w", err)
	}
	//log.Printf("[fuse-rust] confirmation received: %v since write start", time.Since(writeStart))
	if string(resp) != "CONFIRMED" {
		return fmt.Errorf("unexpected confirmation: %q", string(resp))
	}

	if err := <-writeErr; err != nil {
		return err
	}

	return nil
}

// DrainStale sends 'c' to fuserust to clear accumulated statediffs
// without applying them. Used after container initialization.
func (s *RustSyncer) DrainStale() error {
	log.Printf("[fuse-rust] DrainStale() called")
	if err := s.connect(); err != nil {
		return err
	}
	if _, err := s.coreConn.Write([]byte("c")); err != nil {
		s.coreConn = nil
		return fmt.Errorf("send 'c' to fuserust: %w", err)
	}
	log.Printf("[fuse-rust] stale statediffs cleared")
	return nil
}
