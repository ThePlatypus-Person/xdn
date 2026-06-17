package main

import "time"

// Config holds all runtime configuration for the XDN prototype.
// Edit these values to match your environment before running.
type Config struct {
	// Docker image to use for all three containers.
	Image string

	// Port the application inside the container listens on.
	AppPort int
	DBType  string
	DBInitWait time.Duration

	// External ports the Go proxy exposes for each node.
	PrimaryPort int
	Backup1Port int
	Backup2Port int

	// Base directory for volume mounts.
	// Subdirectories primary/, backup1/, backup2/ and diffs/ will be created here.
	VolumeBase string

	// Directory where fuselog_core writes state diffs.
	// fuselog_apply instances read from here.
	DiffDir string

	RefreshInterval time.Duration

	SyncType        string
	FuseCppBin      string
	FuseApplyCppBin string
	FuseSocketFile  string
	FuseDiffFile    string

	FuseRustCoreBin     string
	FuseRustApplyBin    string
	FuseRustCoreSock    string
	FuseRustApplySocks  []string

	LogFuseRust bool
	LogProxy    bool

	// in the Config struct, add:
	DisableWriteRedirect bool
	DisableRenewer       bool
}

func DefaultConfig() Config {
	return Config{
		Image:        "michael2718/bookcatalog-nd:4",
		AppPort:      80,
		DBType:       "sqlite",
		DBInitWait:   30 * time.Second,
		PrimaryPort:  2300,
		Backup1Port:  2301,
		Backup2Port:  2302,
		VolumeBase:   "./volumes",
		DiffDir:      "./volumes/diffs",
		RefreshInterval: 30 * time.Second,

		SyncType:        "rsync",
		FuseCppBin:      "./fusecpp",
		FuseApplyCppBin: "./fusecpp-apply",
		FuseSocketFile:  "./capture.sock",
		FuseDiffFile:    "./statediff.tmp",

		FuseRustCoreBin:    "./fuserust",
		FuseRustApplyBin:   "./fuserust-apply",
		FuseRustCoreSock:   "/tmp/fuserust.sock",
		FuseRustApplySocks: []string{
			"/tmp/fuserust-apply_primary.sock",
			"/tmp/fuserust-apply_backup1.sock",
			"/tmp/fuserust-apply_backup2.sock",
		},

		LogFuseRust: false,
		LogProxy:    false,
		DisableWriteRedirect: false,
		DisableRenewer:       false,
	}
}
