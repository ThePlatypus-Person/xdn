package renewer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/types"
)

// BackupNode holds the mutable state for one backup node's renewal cycle.
type BackupNode struct {
	Role        string
	BaseDir     string // e.g. ./volumes/backup1
	SnapshotDir string // e.g. ./volumes/backup1/snapshot
	currentLive string // "live" or "live2" — whichever is active now
	currentID   string
	currentDBID       string
	currentNetworkID  string
	PortLive    int  // port for the live/ container
	PortLive2   int  // port for the live2/ container
}

type Docker interface {
	StartBackupContainerWithNetwork(ctx context.Context, role, name string, hostPort int, volumeDir, networkName string) (*types.ContainerInfo, error)
	StopOne(ctx context.Context, id string)
	WaitHealthy(hostPort int, timeout time.Duration) error
	RemoveNetwork(ctx context.Context, networkID string)
	WaitForDB(ctx context.Context, containerID string, timeout time.Duration) error
}

// Proxy is the subset of proxy.Proxy the renewer needs.
type Proxy interface {
	SwapTarget(role string, newPort int)
}

type Renewer struct {
	nodes       []*BackupNode
	docker      Docker
	proxy       Proxy
	interval    time.Duration
	mu          sync.Mutex
}

func New(nodes []*BackupNode, docker Docker, proxy Proxy, interval time.Duration) *Renewer {
	return &Renewer{
		nodes:      nodes,
		docker:     docker,
		proxy:      proxy,
		interval:   interval,
	}
}

// Start runs the renewal loop for all backup nodes. Non-blocking.
func (r *Renewer) Start(ctx context.Context, initialContainers map[string]*types.ContainerInfo) {
    for _, node := range r.nodes {
        node.currentLive = "live"
        node.currentID = initialContainers[node.Role].ID
        node.currentDBID = initialContainers[node.Role].DBContainerID
        node.currentNetworkID = initialContainers[node.Role].NetworkID
        go r.loop(ctx, node, initialContainers[node.Role].ID)
    }
}

func (r *Renewer) loop(ctx context.Context, node *BackupNode, currentID string) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newID, newPort, err := r.cycle(ctx, node, currentID)
			if err != nil {
				log.Printf("[renewer:%s] cycle failed: %v", node.Role, err)
				continue
			}
			currentID = newID
			_ = newPort
		}
	}
}

func (r *Renewer) cycle(ctx context.Context, node *BackupNode, oldID string) (newID string, newPort int, err error) {
	select {
	case <-ctx.Done():
		return "", 0, fmt.Errorf("context cancelled")
	default:
	}

	nextLive := "live2"
	if node.currentLive == "live2" {
		nextLive = "live"
	}
	nextLiveDir := filepath.Join(node.BaseDir, nextLive)

	log.Printf("[renewer:%s] starting cycle: snapshot → %s", node.Role, nextLive)

	// Step 1: copy snapshot → nextLiveDir (fresh state).
	if err := os.RemoveAll(nextLiveDir); err != nil {
		return "", 0, fmt.Errorf("remove %s: %w", nextLiveDir, err)
	}
	if err := os.MkdirAll(nextLiveDir, 0o755); err != nil {
		return "", 0, fmt.Errorf("mkdir %s: %w", nextLiveDir, err)
	}
	cmd := exec.Command("rsync", "-a", node.SnapshotDir+"/", nextLiveDir)
	if out, err2 := cmd.CombinedOutput(); err2 != nil {
		return "", 0, fmt.Errorf("rsync snapshot → %s: %w\n%s", nextLive, err2, out)
	}
	log.Printf("[renewer:%s] snapshot copied to %s", node.Role, nextLiveDir)

	// Step 2: pick a fresh port and start the new container.
	if nextLive == "live" {
		newPort = node.PortLive
	} else {
		newPort = node.PortLive2
	}
	newName := fmt.Sprintf("xdn-%s-%s", node.Role, nextLive)

	networkName := fmt.Sprintf("xdn-net-%s", node.Role)
	info, err := r.docker.StartBackupContainerWithNetwork(ctx, node.Role, newName, newPort, nextLiveDir, networkName)
	if err != nil {
		return "", 0, fmt.Errorf("start container: %w", err)
	}
	log.Printf("[renewer:%s] new container %s started on :%d", node.Role, info.ID[:12], newPort)

	// Wait for DB to initialize before checking app health
	if info.DBContainerID != "" {
		log.Printf("[renewer:%s] waiting for DB to be healthy...", node.Role)
		if err := r.docker.WaitForDB(ctx, info.DBContainerID, 60*time.Second); err != nil {
			r.docker.StopOne(ctx, info.ID)
			r.docker.StopOne(ctx, info.DBContainerID)
			return "", 0, fmt.Errorf("DB health check: %w", err)
		}
		log.Printf("[renewer:%s] DB is ready", node.Role)
	}

	// Step 3: wait for new container to be healthy before switching.
	if err := r.docker.WaitHealthy(newPort, 30*time.Second); err != nil {
		r.docker.StopOne(ctx, info.ID)
		return "", 0, fmt.Errorf("health check: %w", err)
	}
	log.Printf("[renewer:%s] new container healthy, swapping proxy target", node.Role)

	// Step 4: atomically swap proxy to new container.
	r.proxy.SwapTarget(node.Role, newPort)

	// Step 5: stop old app container, DB container, remove network, delete old live dir.
	log.Printf("[renewer:%s] stopping old containers — app: %s, db: %s", node.Role, oldID, node.currentDBID)
	r.docker.StopOne(ctx, oldID)
	if node.currentDBID != "" {
		r.docker.StopOne(ctx, node.currentDBID)
	}
	oldLiveDir := filepath.Join(node.BaseDir, node.currentLive)
	if err := os.RemoveAll(oldLiveDir); err != nil {
		log.Printf("[renewer:%s] remove old live dir: %v", node.Role, err)
	}

	node.currentDBID = info.DBContainerID
	node.currentNetworkID = info.NetworkID
	r.mu.Lock()
	node.currentID = info.ID
	node.currentDBID = info.DBContainerID
	node.currentLive = nextLive
	r.mu.Unlock()
	log.Printf("[renewer:%s] cycle complete, active dir: %s", node.Role, nextLive)

	return info.ID, newPort, nil
}

func (r *Renewer) Stop(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, node := range r.nodes {
		if node.currentID != "" {
			r.docker.StopOne(ctx, node.currentID)
		}
		if node.currentDBID != "" {
			r.docker.StopOne(ctx, node.currentDBID)
		}
		if node.currentNetworkID != "" {
			r.docker.RemoveNetwork(ctx, node.currentNetworkID)
		}
	}
}
