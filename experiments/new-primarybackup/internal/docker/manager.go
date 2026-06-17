package docker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/fadhilkurnia/xdn/experiments/new-primarybackup/internal/types"
	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// Manager owns the Docker client and knows how to create/destroy the node containers.
type Manager struct {
	client      *dockerclient.Client
	image       string
	appPort     int
	volumeBase  string
	dbType      DBType
	dbCfg       DBConfig
}

func NewManager(ctx context.Context, image string, appPort int, volumeBase string, dbType DBType) (*Manager, error) {
	cli, err := dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	mgr := &Manager{
		client:     cli,
		image:      image,
		appPort:    appPort,
		volumeBase: volumeBase,
		dbType:     dbType,
		dbCfg:      GetDBConfig(dbType),
	}

	// Pull app image if not present
	if err := mgr.ensureImage(ctx, image); err != nil {
		return nil, err
	}

	// Pull DB image if not SQLite
	if dbType != DBTypeSQLite && GetDBConfig(dbType).Image != "" {
		if err := mgr.ensureImage(ctx, GetDBConfig(dbType).Image); err != nil {
			return nil, err
		}
	}

	return mgr, nil
}

func (m *Manager) ensureImage(ctx context.Context, image string) error {
	_, _, err := m.client.ImageInspectWithRaw(ctx, image)
	if err == nil {
		return nil // already exists
	}

	log.Printf("[docker] pulling image %s...", image)
	reader, err := m.client.ImagePull(ctx, image, dockerimage.PullOptions{})
	if err != nil {
		return fmt.Errorf("ImagePull %s: %w", image, err)
	}
	defer reader.Close()
	io.Copy(io.Discard, reader) // must drain reader for pull to complete
	log.Printf("[docker] pulled %s", image)
	return nil
}

// StartContainers creates and starts all three node containers.
// Returns them in order: [primary, backup1, backup2].
func (m *Manager) StartContainers(ctx context.Context, syncFn func() error, drainFn func() error) ([]*types.ContainerInfo, error) {
	nodes := []struct {
		role     string
		name     string
		hostPort int
	}{
		{"primary", "xdn-primary", 2001},
		{"backup1", "xdn-backup1", 2002},
		{"backup2", "xdn-backup2", 2003},
	}

	primaryLiveDir := filepath.Join(m.volumeBase, "primary", "live")

	if m.dbType == DBTypeSQLite {
		// SQLite: start primary, seed backups, start backups sequentially
		primary, err := m.startOne(ctx, nodes[0].role, nodes[0].name, nodes[0].hostPort, primaryLiveDir)
		if err != nil {
			return nil, fmt.Errorf("start primary: %w", err)
		}
		infos := []*types.ContainerInfo{primary}

		log.Println("[docker] waiting for primary to initialize...")
		time.Sleep(2 * time.Second)

		for _, n := range nodes[1:] {
			destLive := filepath.Join(m.volumeBase, n.role, "live")
			if err := os.MkdirAll(destLive, 0o777); err != nil {
				m.StopContainers(context.Background(), infos)
				return nil, fmt.Errorf("mkdir %s: %w", destLive, err)
			}
			destAbs, _ := filepath.Abs(destLive)
			cmd := exec.Command("rsync", "-a", "--ignore-missing-args", primary.VolumeDir+"/", destAbs)
			if out, err := cmd.CombinedOutput(); err != nil {
				m.StopContainers(context.Background(), infos)
				return nil, fmt.Errorf("seed rsync → %s: %w\n%s", n.role, err, out)
			}
			info, err := m.startOne(ctx, n.role, n.name, n.hostPort, destLive)
			if err != nil {
				m.StopContainers(context.Background(), infos)
				return nil, fmt.Errorf("start %s: %w", n.role, err)
			}
			infos = append(infos, info)
		}
		return infos, nil
	}

	// MySQL/PostgreSQL flow:

	// Step 1: create primary network and start primary DB container
	primaryNetID, err := m.createNetwork(ctx, "xdn-net-primary")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(primaryLiveDir, 0o755); err != nil {
		m.removeNetwork(ctx, primaryNetID)
		return nil, fmt.Errorf("mkdir primary live: %w", err)
	}
	primaryDBID, err := m.startDBContainer(ctx, "xdn-primary-db", "xdn-net-primary", primaryLiveDir)
	if err != nil {
		m.removeNetwork(ctx, primaryNetID)
		return nil, fmt.Errorf("start primary DB: %w", err)
	}

	// Step 2: wait for primary DB to initialize
	log.Printf("[docker] waiting for primary DB to be healthy...")
	if err := m.waitForDBHealthy(ctx, primaryDBID, 120*time.Second); err != nil {
		m.StopOne(context.Background(), primaryDBID)
		m.removeNetwork(context.Background(), primaryNetID)
		return nil, fmt.Errorf("primary DB not healthy: %w", err)
	}

	// Step 3: start primary app container
	primaryApp, err := m.startAppContainer(ctx, "primary", "xdn-primary", nodes[0].hostPort, "xdn-net-primary")
	if err != nil {
		m.StopOne(ctx, primaryDBID)
		m.removeNetwork(ctx, primaryNetID)
		return nil, fmt.Errorf("start primary app: %w", err)
	}

	primary := &types.ContainerInfo{
		ID:            primaryApp,
		Name:          "xdn-primary",
		Role:          "primary",
		HostPort:      nodes[0].hostPort,
		VolumeDir:     primaryLiveDir,
		DBContainerID: primaryDBID,
		NetworkID:     primaryNetID,
	}
	infos := []*types.ContainerInfo{primary}

	// Step 4: seed all snapshots from primary/live with consistency check
	for _, role := range []string{"primary", "backup1", "backup2"} {
		snapDir := filepath.Join(m.volumeBase, role, "snapshot")
		if err := os.MkdirAll(snapDir, 0o755); err != nil {
			m.StopContainers(context.Background(), infos)
			return nil, fmt.Errorf("mkdir snapshot %s: %w", role, err)
		}
	}

	absLive, _ := filepath.Abs(primaryLiveDir)
	snapDirs := []string{
		filepath.Join(m.volumeBase, "primary", "snapshot"),
		filepath.Join(m.volumeBase, "backup1", "snapshot"),
		filepath.Join(m.volumeBase, "backup2", "snapshot"),
	}

	absPrimarySnap, _ := filepath.Abs(snapDirs[0])

	if syncFn != nil {
		// fuse-cpp / fuse-rust: capture all init writes and apply to all snapshots
		log.Printf("[docker] seeding snapshots via syncFn...")
		if err := syncFn(); err != nil {
			m.StopContainers(context.Background(), infos)
			return nil, fmt.Errorf("seed snapshots via syncFn: %w", err)
		}
		if drainFn != nil {
			if err := drainFn(); err != nil {
				log.Printf("[docker] drain stale: %v", err)
			}
		}
	} else {
		// rsync: retry until primary/live and primary/snapshot are consistent
		log.Printf("[docker] seeding snapshots from primary/live (retrying until consistent)...")
		for attempt := 1; ; attempt++ {
			cmd := exec.Command("rsync", "-a", "--delete", "--ignore-missing-args",
			absLive+"/", absPrimarySnap)
			if out, err := cmd.CombinedOutput(); err != nil {
				code := cmd.ProcessState.ExitCode()
				if code != 24 && code != 23 {
					m.StopContainers(context.Background(), infos)
					return nil, fmt.Errorf("seed primary snapshot: %w\n%s", err, out)
				}
			}

			checkCmd := exec.Command("rsync", "-a", "--checksum", "--dry-run",
			"--ignore-missing-args", "--delete",
			absLive+"/", absPrimarySnap)
			out, _ := checkCmd.CombinedOutput()

			if len(strings.TrimSpace(string(out))) == 0 {
				log.Printf("[docker] primary snapshot consistent after %d attempt(s)", attempt)
				break
			}
			log.Printf("[docker] primary snapshot not yet consistent (attempt %d), retrying...", attempt)
			time.Sleep(500 * time.Millisecond)
		}

		// rsync primary/snapshot → backup snapshots
		for _, snapDir := range snapDirs[1:] {
			absSnap, _ := filepath.Abs(snapDir)
			cmd := exec.Command("rsync", "-a", "--delete", "--ignore-missing-args",
			absPrimarySnap+"/", absSnap)
			if out, err := cmd.CombinedOutput(); err != nil {
				code := cmd.ProcessState.ExitCode()
				if code != 24 && code != 23 {
					m.StopContainers(context.Background(), infos)
					return nil, fmt.Errorf("seed backup snapshot: %w\n%s", err, out)
				}
			}
		}
	}


	// Step 5: start backup DB containers concurrently
	type backupResult struct {
		role    string
		name    string
		hostPort int
		dbID    string
		netID   string
		err     error
	}
	resultCh := make(chan backupResult, 2)

	for _, n := range nodes[1:] {
		n := n
		go func() {
			backupLiveDir := filepath.Join(m.volumeBase, n.role, "live")
			if err := os.MkdirAll(backupLiveDir, 0o755); err != nil {
				resultCh <- backupResult{role: n.role, err: fmt.Errorf("mkdir %s: %w", backupLiveDir, err)}
				return
			}
			// seed live from snapshot (snapshot is now consistent with primary/live)
			snapDir := filepath.Join(m.volumeBase, n.role, "snapshot")
			absSnap, _ := filepath.Abs(snapDir)
			cmd := exec.Command("rsync", "-a", "--ignore-missing-args", absSnap+"/", backupLiveDir)
			if out, err := cmd.CombinedOutput(); err != nil {
				code := cmd.ProcessState.ExitCode()
				if code != 24 && code != 23 {
					resultCh <- backupResult{role: n.role, err: fmt.Errorf("seed live %s: %w\n%s", n.role, err, out)}
					return
				}
				log.Printf("[docker] rsync warning (code %d) seeding live %s, continuing", code, n.role)
			}
			netName := fmt.Sprintf("xdn-net-%s", n.role)
			netID, err := m.createNetwork(ctx, netName)
			if err != nil {
				resultCh <- backupResult{role: n.role, err: err}
				return
			}
			dbName := fmt.Sprintf("xdn-%s-db", n.role)
			dbID, err := m.startDBContainer(ctx, dbName, netName, backupLiveDir)
			if err != nil {
				m.removeNetwork(ctx, netID)
				resultCh <- backupResult{role: n.role, err: fmt.Errorf("start %s DB: %w", n.role, err)}
				return
			}
			resultCh <- backupResult{role: n.role, name: n.name, hostPort: n.hostPort, dbID: dbID, netID: netID}
		}()
	}

	// Collect DB start results
	type backupDB struct {
		role     string
		name     string
		hostPort int
		dbID     string
		netID    string
	}
	var backupDBs []backupDB
	for i := 0; i < 2; i++ {
		res := <-resultCh
		if res.err != nil {
			m.StopContainers(context.Background(), infos)
			return nil, res.err
		}
		backupDBs = append(backupDBs, backupDB{
			role:     res.role,
			name:     res.name,
			hostPort: res.hostPort,
			dbID:     res.dbID,
			netID:    res.netID,
		})
	}

	// Step 6: wait for backup DBs to initialize
	log.Printf("[docker] waiting for backup DBs to be healthy...")
	for _, b := range backupDBs {
		if err := m.waitForDBHealthy(ctx, b.dbID, 120*time.Second); err != nil {
			m.StopContainers(context.Background(), infos)
			return nil, fmt.Errorf("backup DB %s not healthy: %w", b.role, err)
		}
	}

	// Step 7: start backup app containers concurrently
	type appResult struct {
		info *types.ContainerInfo
		err  error
	}
	appCh := make(chan appResult, 2)

	for _, b := range backupDBs {
		b := b
		go func() {
			netName := fmt.Sprintf("xdn-net-%s", b.role)
			appID, err := m.startAppContainer(ctx, b.role, b.name, b.hostPort, netName)
			if err != nil {
				appCh <- appResult{err: fmt.Errorf("start %s app: %w", b.role, err)}
				return
			}
			appCh <- appResult{info: &types.ContainerInfo{
				ID:            appID,
				Name:          b.name,
				Role:          b.role,
				HostPort:      b.hostPort,
				VolumeDir:     filepath.Join(m.volumeBase, b.role, "live"),
				DBContainerID: b.dbID,
				NetworkID:     b.netID,
			}}
		}()
	}

	for i := 0; i < 2; i++ {
		res := <-appCh
		if res.err != nil {
			m.StopContainers(context.Background(), infos)
			return nil, res.err
		}
		infos = append(infos, res.info)
	}

	return infos, nil
}

func (m *Manager) startOne(ctx context.Context, role, name string, hostPort int, liveDir string) (*types.ContainerInfo, error) {
	snapDir := filepath.Join(m.volumeBase, role, "snapshot")
	for _, d := range []string{liveDir, snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	containerPort := nat.Port(fmt.Sprintf("%d/tcp", m.appPort))
	info := &types.ContainerInfo{
		Role:      role,
		Name:      name,
		HostPort:  hostPort,
		VolumeDir: liveDir,
	}

	if m.dbType == DBTypeSQLite {
		// SQLite: mount live dir directly into app container
		absVol, err := filepath.Abs(liveDir)
		if err != nil {
			return nil, err
		}
		resp, err := m.client.ContainerCreate(
			ctx,
			&container.Config{
				Image:        m.image,
				Env:          m.dbCfg.AppEnv,
				ExposedPorts: nat.PortSet{containerPort: struct{}{}},
				User:         fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			},
			&container.HostConfig{
				PortBindings: nat.PortMap{
					containerPort: []nat.PortBinding{
						{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d", hostPort)},
					},
				},
				Mounts: []mount.Mount{
					{
						Type:   mount.TypeBind,
						Source: absVol,
						Target: m.dbCfg.AppMountPath,
					},
				},
			},
			nil, nil, name,
		)
		if err != nil {
			return nil, fmt.Errorf("ContainerCreate: %w", err)
		}
		if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
			return nil, fmt.Errorf("ContainerStart: %w", err)
		}
		info.ID = resp.ID
	} else {
		// MySQL/PostgreSQL: create network, start DB container, then app container
		networkName := fmt.Sprintf("xdn-net-%s", role)

		netID, err := m.createNetwork(ctx, networkName)
		if err != nil {
			return nil, err
		}
		info.NetworkID = netID

		dbName := fmt.Sprintf("%s-db", name)
		dbContainerID, err := m.startDBContainer(ctx, dbName, networkName, liveDir)
		if err != nil {
			m.removeNetwork(ctx, netID)
			return nil, fmt.Errorf("start DB container: %w", err)
		}
		info.DBContainerID = dbContainerID

		// Expose DB port to host for health polling
		if err := m.waitForDBHealthy(ctx, dbContainerID, 60*time.Second); err != nil {
			m.StopOne(ctx, dbContainerID)
			m.removeNetwork(ctx, netID)
			return nil, fmt.Errorf("DB not ready: %w", err)
		}

		// Start app container on same network
		resp, err := m.client.ContainerCreate(
			ctx,
			&container.Config{
				Image:        m.image,
				Env:          m.dbCfg.AppEnv,
				ExposedPorts: nat.PortSet{containerPort: struct{}{}},
				User:         fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			},
			&container.HostConfig{
				PortBindings: nat.PortMap{
					containerPort: []nat.PortBinding{
						{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d", hostPort)},
					},
				},
			},
			&network.NetworkingConfig{
				EndpointsConfig: map[string]*network.EndpointSettings{
					networkName: {},
				},
			},
			nil, name,
		)
		if err != nil {
			m.StopOne(ctx, dbContainerID)
			m.removeNetwork(ctx, netID)
			return nil, fmt.Errorf("ContainerCreate app: %w", err)
		}
		if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
			m.StopOne(ctx, dbContainerID)
			m.removeNetwork(ctx, netID)
			return nil, fmt.Errorf("ContainerStart app: %w", err)
		}
		info.ID = resp.ID
	}

	return info, nil
}

func (m *Manager) startAppContainer(ctx context.Context, role, name string, hostPort int, networkName string) (string, error) {
	containerPort := nat.Port(fmt.Sprintf("%d/tcp", m.appPort))

	resp, err := m.client.ContainerCreate(
		ctx,
		&container.Config{
			Image:        m.image,
			Env:          m.dbCfg.AppEnv,
			ExposedPorts: nat.PortSet{containerPort: struct{}{}},
			User:         fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				containerPort: []nat.PortBinding{
					{HostIP: "127.0.0.1", HostPort: fmt.Sprintf("%d", hostPort)},
				},
			},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				networkName: {},
			},
		},
		nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("ContainerCreate app %s: %w", name, err)
	}
	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("ContainerStart app %s: %w", name, err)
	}
	return resp.ID, nil
}

func (m *Manager) StartBackupContainer(ctx context.Context, role, name string, hostPort int, volumeDir string) (*types.ContainerInfo, error) {
	return m.startOne(ctx, role, name, hostPort, volumeDir)
}

func (m *Manager) StartBackupContainerWithNetwork(ctx context.Context, role, name string, hostPort int, volumeDir, networkName string) (*types.ContainerInfo, error) {
	if m.dbType == DBTypeSQLite {
		return m.startOne(ctx, role, name, hostPort, volumeDir)
	}

	// Start DB container on existing network
	dbName := fmt.Sprintf("%s-db", name)
	dbID, err := m.startDBContainer(ctx, dbName, networkName, volumeDir)
	if err != nil {
		return nil, fmt.Errorf("start DB container: %w", err)
	}

	if err := m.waitForDBHealthy(ctx, dbID, 60*time.Second); err != nil {
		m.StopOne(ctx, dbID)
		return nil, fmt.Errorf("DB not ready: %w", err)
	}

	appID, err := m.startAppContainer(ctx, role, name, hostPort, networkName)
	if err != nil {
		m.StopOne(ctx, dbID)
		return nil, fmt.Errorf("start app container: %w", err)
	}

	return &types.ContainerInfo{
		ID:            appID,
		Name:          name,
		Role:          role,
		HostPort:      hostPort,
		VolumeDir:     volumeDir,
		DBContainerID: dbID,
		NetworkID:     "",  // network is reused, not owned by this container
	}, nil
}


// StopContainers stops and removes all containers.
// Uses a background context so it always runs even during shutdown.
func (m *Manager) StopContainers(ctx context.Context, containers []*types.ContainerInfo) {
	// Stop all app and DB containers first
	for _, c := range containers {
		m.StopOne(ctx, c.ID)
		if c.DBContainerID != "" {
			m.StopOne(ctx, c.DBContainerID)
		}
	}
	// Then remove networks after all containers are stopped
	for _, c := range containers {
		if c.NetworkID != "" {
			m.removeNetwork(ctx, c.NetworkID)
		}
	}
}

func (m *Manager) StopOne(ctx context.Context, id string) {
	if id == "" {
		return
	}
	timeout := 10
	if err := m.client.ContainerStop(ctx, id, container.StopOptions{Timeout: &timeout}); err != nil {
		if !strings.Contains(err.Error(), "No such container") {
			log.Printf("[docker] stop %s: %v", id[:12], err)
		}
	}
	if err := m.client.ContainerRemove(ctx, id, container.RemoveOptions{}); err != nil {
		if !strings.Contains(err.Error(), "No such container") {
			log.Printf("[docker] remove %s: %v", id[:12], err)
		}
	}
}

func (m *Manager) createNetwork(ctx context.Context, name string) (string, error) {
	resp, err := m.client.NetworkCreate(ctx, name, network.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("NetworkCreate %s: %w", name, err)
	}
	return resp.ID, nil
}

func (m *Manager) removeNetwork(ctx context.Context, networkID string) {
	if networkID == "" {
		return
	}
	if err := m.client.NetworkRemove(ctx, networkID); err != nil {
		if !strings.Contains(err.Error(), "active endpoints") &&
		!strings.Contains(err.Error(), "not found") {
			log.Printf("[docker] remove network %s: %v", networkID, err)
		}
	}
}

func (m *Manager) WaitHealthy(hostPort int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/", hostPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("container on :%d not healthy after %s", hostPort, timeout)
}

func (m *Manager) startDBContainer(ctx context.Context, name, networkName, liveDir string) (string, error) {
	absVol, err := filepath.Abs(liveDir)
	if err != nil {
		return "", err
	}

	resp, err := m.client.ContainerCreate(
		ctx,
		&container.Config{
			Image: m.dbCfg.Image,
			Env:   m.dbCfg.DBEnv,
			User:  fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			Healthcheck: m.dbCfg.Healthcheck,
		},
		&container.HostConfig{
			Mounts: []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: absVol,
					Target: m.dbCfg.MountPath,
				},
				{
					Type:     mount.TypeBind,
					Source:   "/etc/passwd",
					Target:   "/etc/passwd",
					ReadOnly: true,
				},
			},
		},
		&network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				networkName: {Aliases: []string{"db"}},
			},
		},
		nil, name,
	)
	if err != nil {
		return "", fmt.Errorf("ContainerCreate DB %s: %w", name, err)
	}

	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("ContainerStart DB %s: %w", name, err)
	}

	return resp.ID, nil
}

func (m *Manager) waitForDBHealthy(ctx context.Context, containerID string, timeout time.Duration) error {
	if m.dbType == DBTypeSQLite {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		inspect, err := m.client.ContainerInspect(ctx, containerID)
		if err != nil {
			return fmt.Errorf("inspect container: %w", err)
		}
		if inspect.State.Health != nil && inspect.State.Health.Status == "healthy" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("DB container %s not healthy after %s", containerID[:12], timeout)
}

func (m *Manager) WaitForDB(ctx context.Context, containerID string, timeout time.Duration) error {
    return m.waitForDBHealthy(ctx, containerID, timeout)
}

func (m *Manager) RemoveNetwork(ctx context.Context, networkID string) {
	m.removeNetwork(ctx, networkID)
}
