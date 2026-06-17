package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/docker"
	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/proxy"
	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/renewer"
	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/rsync"
	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/types"
	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/fuse"
)

func main() {
	cfg := DefaultConfig()

	flag.StringVar(&cfg.DBType, "db", cfg.DBType, "database type: sqlite, mysql, postgres")
	flag.StringVar(&cfg.SyncType, "sync", cfg.SyncType, "sync type: rsync, fuse-cpp, fuse-rust")
	flag.DurationVar(&cfg.RefreshInterval, "refresh-interval", cfg.RefreshInterval, "how often backup containers are renewed e.g. 30s, 1m")

	flag.BoolVar(&cfg.LogFuseRust, "log-fuse", cfg.LogFuseRust, "enable fuse-rust timing logs")
	flag.BoolVar(&cfg.LogProxy, "log-proxy", cfg.LogProxy, "enable proxy timing logs")
	flag.BoolVar(&cfg.DisableWriteRedirect, "no-redirect", cfg.DisableWriteRedirect, "disable write redirection to primary (each proxy writes to its own container)")
	flag.BoolVar(&cfg.DisableRenewer, "no-renewer", cfg.DisableRenewer, "disable the blue/green backup renewal loop")
	flag.Parse()

	log.Println("=== XDN Prototype ===")
	log.Printf("Image:   %s", cfg.Image)
	log.Printf("Ports:   primary=:%d  backup1=:%d  backup2=:%d",
		cfg.PrimaryPort, cfg.Backup1Port, cfg.Backup2Port)
	log.Printf("DB:      %s", cfg.DBType)
	log.Printf("Volumes: %s", cfg.VolumeBase)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		received := <-sig
		log.Printf("Signal %s received, shutting down...", received)
		cancel()
	}()

	// 1. Set up rsync paths
	// 1. Set up syncer based on --sync flag
	var syncFn func() error
	var lockFn func()
	var unlockFn func()
	var fuseProc *fuse.CppProcess
	var cppSyncer *fuse.CppSyncer
	var rustSyncer *fuse.RustSyncer

	switch cfg.SyncType {
	case "rsync":
		s := &rsync.Syncer{
			PrimaryLive:     filepath.Join(cfg.VolumeBase, "primary", "live"),
			PrimarySnapshot: filepath.Join(cfg.VolumeBase, "primary", "snapshot"),
			SnapshotDirs: []string{
				filepath.Join(cfg.VolumeBase, "primary", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup1", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup2", "snapshot"),
			},
			BatchPath: filepath.Join(cfg.VolumeBase, "batch"),
			DBType: cfg.DBType,
		}
		syncFn   = s.Sync
		lockFn   = s.Lock
		unlockFn = s.Unlock

	case "fuse-cpp":
		fuseProc = &fuse.CppProcess{
			Bin:        cfg.FuseCppBin,
			MountPoint: filepath.Join(cfg.VolumeBase, "primary", "live"),
			SocketPath: cfg.FuseSocketFile,
		}
		if err := fuseProc.Start(ctx); err != nil {
			log.Fatalf("start fuselogv2: %v", err)
		}
		defer fuseProc.Stop()

		cppSyncer = &fuse.CppSyncer{
			SocketPath: cfg.FuseSocketFile,
			DiffFile:   cfg.FuseDiffFile,
			ApplyBin:   cfg.FuseApplyCppBin,
			SnapshotDirs: []string{
				filepath.Join(cfg.VolumeBase, "primary", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup1", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup2", "snapshot"),
			},
		}
		syncFn   = cppSyncer.Sync
		lockFn   = cppSyncer.Lock
		unlockFn = cppSyncer.Unlock
		defer cppSyncer.Close()
	case "fuse-rust":
		rustProc := &fuse.RustProcess{
			CoreBin:    cfg.FuseRustCoreBin,
			ApplyBin:   cfg.FuseRustApplyBin,
			MountPoint: filepath.Join(cfg.VolumeBase, "primary", "live"),
			CoreSocket: cfg.FuseRustCoreSock,
			ApplySockets: cfg.FuseRustApplySocks,
			SnapshotDirs: []string{
				filepath.Join(cfg.VolumeBase, "primary", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup1", "snapshot"),
				filepath.Join(cfg.VolumeBase, "backup2", "snapshot"),
			},
		}
		if err := rustProc.Start(ctx); err != nil {
			log.Fatalf("start fuse-rust: %v", err)
		}
		defer rustProc.Stop()

		rustSyncer = &fuse.RustSyncer{
			CoreSocket:   cfg.FuseRustCoreSock,
			ApplySockets: cfg.FuseRustApplySocks,
			LogTiming:    cfg.LogFuseRust,
		}
		syncFn   = rustSyncer.Sync
		lockFn   = rustSyncer.Lock
		unlockFn = rustSyncer.Unlock
		defer rustSyncer.Close()

	default:
		log.Fatalf("unknown sync type: %s (valid: rsync, fuse-cpp)", cfg.SyncType)
	}

	// ── 2. Start 3 docker containers ───────────────────────────────────────
	mgr, err := docker.NewManager(ctx, cfg.Image, cfg.AppPort, cfg.VolumeBase, docker.DBType(cfg.DBType))
	if err != nil {
		log.Fatalf("docker manager init: %v", err)
	}

	var containers []*types.ContainerInfo
	defer func() {
		if len(containers) > 0 {
			log.Println("Stopping containers...")
			mgr.StopContainers(context.Background(), containers)
		}
	}()

	var drainFn func() error
	containers, err = mgr.StartContainers(ctx, syncFn, drainFn)
	if err != nil {
		log.Printf("start containers: %v", err)
		return
	}

	// ── 3. Start the reverse proxy servers ────────────────────────────────
	p := proxy.New([]proxy.NodeConfig{
		{
			Role:          "primary",
			ListenPort:    cfg.PrimaryPort,
			ContainerPort: containers[0].HostPort,
			SyncFn:    syncFn,
			LockFn:    lockFn,
			UnlockFn:  unlockFn,
			LogTiming: cfg.LogProxy,
			DisableWriteRedirect: cfg.DisableWriteRedirect,
		},
		{
			Role:          "backup1",
			ListenPort:    cfg.Backup1Port,
			ContainerPort: containers[1].HostPort,
			SyncFn:   syncFn,
			LockFn:   lockFn,
			UnlockFn: unlockFn,
			LogTiming: cfg.LogProxy,
			DisableWriteRedirect: cfg.DisableWriteRedirect,
		},
		{
			Role:          "backup2",
			ListenPort:    cfg.Backup2Port,
			ContainerPort: containers[2].HostPort,
			SyncFn:   syncFn,
			LockFn:   lockFn,
			UnlockFn: unlockFn,
			LogTiming: cfg.LogProxy,
			DisableWriteRedirect: cfg.DisableWriteRedirect,
		},
	})

	if err := p.Start(); err != nil {
		log.Fatalf("proxy start: %v", err)
	}
	defer func() {
		log.Println("Stopping proxies...")
		p.Stop()
	}()
	if err := p.OpenSwitchoverLog("./switchovers.log"); err != nil {
		log.Fatalf("switchover log: %v", err)
	}
	defer p.CloseSwitchoverLog()

	log.Println("Ready.")
	log.Printf("  Primary  → http://localhost:%d  (reads + writes)", cfg.PrimaryPort)
	log.Printf("  Backup 1 → http://localhost:%d  (reads only; writes → primary)", cfg.Backup1Port)
	log.Printf("  Backup 2 → http://localhost:%d  (reads only; writes → primary)", cfg.Backup2Port)

	// ── 4. Start backup renewal loop ──────────────────────────────────────
	if !cfg.DisableRenewer {
		backupNodes := []*renewer.BackupNode{
			{
				Role:        "backup1",
				BaseDir:     filepath.Join(cfg.VolumeBase, "backup1"),
				SnapshotDir: filepath.Join(cfg.VolumeBase, "backup1", "snapshot"),
				PortLive:    containers[1].HostPort,
				PortLive2:   containers[1].HostPort + 100,
			},
			{
				Role:        "backup2",
				BaseDir:     filepath.Join(cfg.VolumeBase, "backup2"),
				SnapshotDir: filepath.Join(cfg.VolumeBase, "backup2", "snapshot"),
				PortLive:    containers[2].HostPort,
				PortLive2:   containers[2].HostPort + 100,
			},
		}

		initialContainers := map[string]*types.ContainerInfo{
			"backup1": containers[1],
			"backup2": containers[2],
		}

		r := renewer.New(backupNodes, mgr, p, cfg.RefreshInterval)
		r.Start(ctx, initialContainers)
		defer func() {
			log.Println("Stopping renewer containers...")
			r.Stop(context.Background())
		}()
	}

	// ── Wait for context to be cancelled (signal received) ────────────────
	<-ctx.Done()
}
