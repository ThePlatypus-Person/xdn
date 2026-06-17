package fuse

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
)

// CppSyncer implements Lock/Unlock/Sync/DrainStale using fusecpp binaries.
// It maintains a persistent connection to fusecpp and runs fusecpp-apply
// as a one-shot process per sync.
type CppSyncer struct {
	SocketPath   string   // fusecpp socket path
	DiffFile     string   // path to write statediff for fusecpp-apply
	ApplyBin     string   // path to fusecpp-apply binary
	SnapshotDirs []string // snapshot dirs to apply to

	mu            sync.Mutex
	conn          net.Conn
	syncCount     int
	totalDuration time.Duration
}

func (s *CppSyncer) Lock() {
	s.mu.Lock()
}

func (s *CppSyncer) Unlock() {
	s.mu.Unlock()
}

// Close closes the persistent connection to fusecpp.
func (s *CppSyncer) Close() {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// connect ensures a persistent connection to fusecpp.
func (s *CppSyncer) connect() error {
	if s.conn != nil {
		return nil
	}
	conn, err := net.Dial("unix", s.SocketPath)
	if err != nil {
		return fmt.Errorf("dial fusecpp socket %s: %w", s.SocketPath, err)
	}
	s.conn = conn
	return nil
}

// Sync captures the statediff from fusecpp and applies it to all snapshot dirs.
func (s *CppSyncer) Sync() error {
	//log.Printf("[fuse-cpp] Sync() called")
	start := time.Now()

	// Step 1: ensure persistent connection
	if err := s.connect(); err != nil {
		return err
	}

	// Step 2: send 'g' to request statediff
	if _, err := s.conn.Write([]byte("g")); err != nil {
		s.conn = nil
		return fmt.Errorf("send 'g' to fusecpp: %w", err)
	}
	//log.Printf("[fuse-cpp] sent 'g'")

	// Step 3: read 8-byte little-endian size
	var totalSize uint64
	if err := binary.Read(s.conn, binary.LittleEndian, &totalSize); err != nil {
		s.conn = nil
		return fmt.Errorf("read statediff size: %w", err)
	}
	//log.Printf("[fuse-cpp] payload size: %d bytes", totalSize)

	if totalSize == 0 {
		return nil
	}

	// Step 4: read full blob
	blob := make([]byte, totalSize)
	if _, err := io.ReadFull(s.conn, blob); err != nil {
		s.conn = nil
		return fmt.Errorf("read statediff blob (%d bytes): %w", totalSize, err)
	}
	//log.Printf("[fuse-cpp] payload received")

	// Step 5: write blob to diff file
	if err := os.MkdirAll(dirOf(s.DiffFile), 0o755); err != nil {
		return fmt.Errorf("mkdir for diff file: %w", err)
	}
	if err := os.WriteFile(s.DiffFile, blob, 0o644); err != nil {
		return fmt.Errorf("write diff file: %w", err)
	}

	// Step 6: run fusecpp-apply for each snapshot dir (one-shot)
	for _, dest := range s.SnapshotDirs {
		target := dest
		//log.Printf("[fuse-cpp] running fusecpp-apply → %s", dest)
		if len(target) == 0 || target[len(target)-1] != '/' {
			target += "/"
		}
		cmd := exec.Command(s.ApplyBin, target, "--statediff="+s.DiffFile)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("fusecpp-apply → %s: %w\n%s", dest, err, out)
		}
		//log.Printf("[fuse-cpp] apply done → %s", dest)
	}

	elapsed := time.Since(start)
	s.syncCount++
	s.totalDuration += elapsed
	if s.syncCount%100 == 0 {
		avg := s.totalDuration / time.Duration(s.syncCount)
		log.Printf("[fuse-cpp] average sync time over %d requests: %v", s.syncCount, avg)
	}

	return nil
}

// DrainStale sends 'c' to fusecpp to clear accumulated statediffs
// without applying them. Used after container initialization.
func (s *CppSyncer) DrainStale() error {
	log.Printf("[fuse-cpp] DrainStale() called")
	if err := s.connect(); err != nil {
		return err
	}
	if _, err := s.conn.Write([]byte("c")); err != nil {
		s.conn = nil
		return fmt.Errorf("send 'c' to fusecpp: %w", err)
	}
	log.Printf("[fuse-cpp] stale statediffs cleared")
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
