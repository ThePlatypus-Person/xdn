package types

type ContainerInfo struct {
	ID            string
	Name          string
	Role          string
	HostPort      int
	VolumeDir     string
	DBContainerID string
	NetworkID     string
}
