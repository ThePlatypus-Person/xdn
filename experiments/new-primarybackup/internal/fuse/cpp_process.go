package fuse

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// CppProcess manages the fusecpp subprocess.
type CppProcess struct {
	Bin        string // path to fusecpp binary
	MountPoint string // directory to mount over
	SocketPath string // unix socket path

	cmd *exec.Cmd
}

// Start launches fusecpp and waits for its unix socket to be ready.
func (p *CppProcess) Start(ctx context.Context) error {
	absMount, err := filepath.Abs(p.MountPoint)
	if err != nil {
		return fmt.Errorf("abs mount point: %w", err)
	}
	p.MountPoint = absMount

	absSocket, err := filepath.Abs(p.SocketPath)
	if err != nil {
		return fmt.Errorf("abs socket path: %w", err)
	}
	p.SocketPath = absSocket

	if err := os.MkdirAll(p.MountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point %s: %w", p.MountPoint, err)
	}

	p.cmd = exec.CommandContext(ctx,
		"sudo", "--preserve-env=FUSELOG_SOCKET_FILE",
		p.Bin, "-f", "-o", "allow_other", p.MountPoint,
	)
	p.cmd.Env = append(os.Environ(), "FUSELOG_SOCKET_FILE="+p.SocketPath)
	logFile, err := os.Create("./fusecpp.log")
	if err != nil {
		return fmt.Errorf("create fusecpp log file: %w", err)
	}
	p.cmd.Stdout = logFile
	p.cmd.Stderr = logFile

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("start fusecpp: %w", err)
	}
	log.Printf("[fuse-cpp] started fusecpp (pid=%d) mounting %s", p.cmd.Process.Pid, p.MountPoint)

	if err := p.waitForSocket(30 * time.Second); err != nil {
		p.Stop()
		return fmt.Errorf("fusecpp socket not ready: %w", err)
	}
	log.Printf("[fuse-cpp] socket ready at %s", p.SocketPath)
	return nil
}

// Stop sends SIGINT to fusecpp and unmounts the FUSE filesystem.
func (p *CppProcess) Stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	log.Printf("[fuse-cpp] stopping fusecpp (pid=%d)", p.cmd.Process.Pid)
	exec.Command("sudo", "kill", "-INT", fmt.Sprintf("%d", p.cmd.Process.Pid)).Run()

	umount := exec.Command("sudo", "fusermount3", "-u", p.MountPoint)
	if out, err := umount.CombinedOutput(); err != nil {
		umount2 := exec.Command("sudo", "fusermount", "-u", p.MountPoint)
		if out2, err2 := umount2.CombinedOutput(); err2 != nil {
			log.Printf("[fuse-cpp] unmount failed: %v\n%s\n%s", err, out, out2)
		}
	}
}

// waitForSocket polls until the socket file appears, chmods it, and verifies connectivity.
func (p *CppProcess) waitForSocket(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(p.SocketPath); err == nil {
			exec.Command("sudo", "chmod", "777", p.SocketPath).Run()
			conn, err := net.Dial("unix", p.SocketPath)
			if err == nil {
				conn.Close()
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("socket %s not available after %s", p.SocketPath, timeout)
}
