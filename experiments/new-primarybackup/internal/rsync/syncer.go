package rsync

import (
	"fmt"
//	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Syncer struct {
	PrimaryLive     string
	PrimarySnapshot string
	SnapshotDirs    []string // all snapshot dirs including primary's
	BatchPath       string   // e.g. ./batch
	mu              sync.Mutex
	syncCount       int
	totalDuration   time.Duration
	DBType string
}

func (s *Syncer) Lock() {
	s.mu.Lock()
}

func (s *Syncer) Unlock() {
	s.mu.Unlock()
}

// Sync generates a batch file from the diff between primary live and primary
// snapshot, then applies it to all snapshot dirs.
func (s *Syncer) Sync() error {
	start := time.Now()
	if err := os.MkdirAll(s.BatchPath, 0o755); err != nil {
		return fmt.Errorf("mkdir batch path: %w", err)
	}

	batchFile := s.BatchPath + "/stateDiff"

	// Step 1: compare primary/live vs primary/snapshot → write batch file.
	// primary/snapshot acts as the "reference" (what snapshots currently are).
	// primary/live is the source of truth (what they should become).
	args := []string{
		"rsync", "-a", "--delete",
		"--ignore-missing-args",
	}
	switch s.DBType {
	case "sqlite":
		args = append(
			args,
			"--exclude=*.db-journal",
			"--exclude=*.db-wal",
			"--exclude=*.db-shm",
		)
	case "postgres", "mysql":
		// no excludes — all DB files must stay consistent for seeding
	}
	args = append(args, "--write-batch="+batchFile, s.PrimaryLive+"/", s.PrimarySnapshot)
	writeCmd := exec.Command(args[0], args[1:]...)

	if out, err := writeCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("WRITE-BATCH failed: %w\n%s", err, out)
	}

	// Ensure batch file is fully flushed to disk before read-batch.
	syncCmd := exec.Command("sync", batchFile)
	if out, err := syncCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("sync batch file: %w\n%s", err, out)
	}

	// Step 2: apply the batch file to all snapshot dirs.
	for _, dest := range s.SnapshotDirs {
		readCmd := exec.Command("rsync", "-a", "--read-batch="+batchFile, dest+"/")
		if out, err := readCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("READ-BATCH failed → %s: %w\n%s", dest, err, out)
		}
	}

	elapsed := time.Since(start)
	s.syncCount++
	s.totalDuration += elapsed

	/*
	if s.syncCount%100 == 0 {
		avg := s.totalDuration / time.Duration(s.syncCount)
		log.Printf("[rsync] average sync time over %d requests: %v", s.syncCount, avg)
	}
	*/
	return nil
}
