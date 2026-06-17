package fuse

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// RustProcess manages the fuserust subprocess and all fuserust-apply subprocesses.
type RustProcess struct {
	CoreBin     string   // path to fuserust binary
	ApplyBin    string   // path to fuserust-apply binary
	MountPoint  string   // directory fuserust mounts over
	CoreSocket  string   // socket path for fuserust
	ApplySockets []string // socket paths for each fuserust-apply instance
	SnapshotDirs []string // snapshot dirs, one per fuserust-apply

	coreCmd  *exec.Cmd
	applyCmds []*exec.Cmd
}

// Start launches fuserust and all fuserust-apply instances.
func (p *RustProcess) Start(ctx context.Context) error {
	absMount, err := filepath.Abs(p.MountPoint)
	if err != nil {
		return fmt.Errorf("abs mount point: %w", err)
	}
	p.MountPoint = absMount

	if err := os.MkdirAll(p.MountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point %s: %w", p.MountPoint, err)
	}

	// Start fuserust with sudo (required for allow_other)
	p.coreCmd = exec.CommandContext(ctx, p.CoreBin, "--foreground", p.MountPoint)
	p.coreCmd.Env = append(
		os.Environ(),
		"FUSELOG_SOCKET_FILE="+p.CoreSocket,
		"ADAPTIVE_DEV_MODE=false",
		"FUSELOG_COMPRESSION=true",
		"FUSELOG_PRUNE=false",
		"WRITE_COALESCING=true",
		"ADAPTIVE_COMPRESSION=false",
		"RUST_LOG=info",
	)
	coreLog, _ := os.Create("./fuserust_core.log")
	p.coreCmd.Stdout = coreLog
	p.coreCmd.Stderr = coreLog

	if err := p.coreCmd.Start(); err != nil {
		return fmt.Errorf("start fuserust: %w", err)
	}
	log.Printf("[fuse-rust] core started (pid=%d) mounting %s", p.coreCmd.Process.Pid, p.MountPoint)

	// Wait for core socket to be ready and chmod it
	if err := p.waitForSocketFile(p.CoreSocket, 30*time.Second); err != nil {
		p.Stop()
		return fmt.Errorf("fuselog_core socket not ready: %w", err)
	}
	log.Printf("[fuse-rust] core socket ready at %s", p.CoreSocket)

	// Start one fuserust-apply per snapshot dir
	for i, snapDir := range p.SnapshotDirs {
		absSnap, err := filepath.Abs(snapDir)
		if err != nil {
			p.Stop()
			return fmt.Errorf("abs snapshot dir: %w", err)
		}
		if err := os.MkdirAll(absSnap, 0o755); err != nil {
			p.Stop()
			return fmt.Errorf("mkdir snapshot %s: %w", absSnap, err)
		}

		sockPath := p.ApplySockets[i]
		applyCmd := exec.CommandContext(ctx, p.ApplyBin,
			absSnap,
			"--applySocket="+sockPath,
		)
		applyCmd.Env = append(os.Environ(), "RUST_LOG=info")
		applyLog, _ := os.Create(fmt.Sprintf("./fuserust_apply_%d.log", i))
		applyCmd.Stdout = applyLog
		applyCmd.Stderr = applyLog

		if err := applyCmd.Start(); err != nil {
			p.Stop()
			return fmt.Errorf("start fuserust-apply[%d]: %w", i, err)
		}
		log.Printf("[fuse-rust] apply[%d] started (pid=%d) → %s", i, applyCmd.Process.Pid, absSnap)
		p.applyCmds = append(p.applyCmds, applyCmd)

		// Wait for apply socket to appear
		if err := p.waitForSocketFile(sockPath, 15*time.Second); err != nil {
			p.Stop()
			return fmt.Errorf("fuserust-apply[%d] socket not ready: %w", i, err)
		}
		log.Printf("[fuse-rust] apply[%d] socket ready at %s", i, sockPath)
	}

	return nil
}

// Stop kills all fuselog processes and unmounts the FUSE filesystem.
func (p *RustProcess) Stop() {
	// Stop apply processes first
	for i, cmd := range p.applyCmds {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Signal(os.Interrupt)
			log.Printf("[fuse-rust] stopped apply[%d]", i)
		}
	}

	// Stop core
	p.coreCmd.Process.Signal(os.Interrupt)
	log.Printf("[fuse-rust] stopped core")

	// Unmount
	umount := exec.Command("fusermount3", "-u", p.MountPoint)
	if out, err := umount.CombinedOutput(); err != nil {
		umount2 := exec.Command("fusermount", "-u", p.MountPoint)
		if out2, err2 := umount2.CombinedOutput(); err2 != nil {
			log.Printf("[fuse-rust] unmount failed: %v\n%s\n%s", err, out, out2)
		}
	}

	// Clean up socket files
	for _, sock := range p.ApplySockets {
		os.Remove(sock)
	}
}

func (p *RustProcess) waitForSocketFile(socketPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("socket file %s did not appear after %s", socketPath, timeout)
}
