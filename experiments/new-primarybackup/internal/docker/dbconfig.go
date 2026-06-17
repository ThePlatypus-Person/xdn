package docker

import (
	"time"

	"github.com/docker/docker/api/types/container"
)

// DBType represents the database backend.
type DBType string

const (
	DBTypeSQLite   DBType = "sqlite"
	DBTypeMySQL    DBType = "mysql"
	DBTypePostgres DBType = "postgres"
)

// DBConfig holds all DB-specific settings for a given DBType.
type DBConfig struct {
	// DB container image. Empty for SQLite (no sidecar needed).
	Image string
	// Port the DB listens on inside the container.
	Port int
	// Environment variables for the DB container.
	DBEnv []string
	// Environment variables passed to the app container.
	AppEnv []string
	// Mount path inside the DB container for data.
	MountPath string
	// Mount path inside the app container. Only used for SQLite.
	AppMountPath string
	Healthcheck  *container.HealthConfig
}

var dbConfigs = map[DBType]DBConfig{
	DBTypeSQLite: {
		Image:        "",
		Port:         80,
		DBEnv:        nil,
		AppEnv:       []string{"DB_TYPE=sqlite", "ENABLE_WAL=true"},
		MountPath:    "",
		AppMountPath: "/app/data",
		Healthcheck: nil,
	},
	DBTypeMySQL: {
		Image:        "mysql:8.0.41-debian",
		Port:         3306,
		DBEnv: []string{
			"MYSQL_DATABASE=books",
			"MYSQL_ROOT_PASSWORD=root",
		},
		AppEnv: []string{
			"DB_TYPE=mysql",
			"DB_HOST=db",
		},
		MountPath:    "/var/lib/mysql",
		AppMountPath: "",
		Healthcheck: &container.HealthConfig{
			Test:     []string{"CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "--silent"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
	},
	DBTypePostgres: {
		Image:        "postgres:17.4-bookworm",
		Port:         5432,
		DBEnv: []string{
			"POSTGRES_DB=books",
			"POSTGRES_PASSWORD=root",
		},
		AppEnv: []string{
			"DB_TYPE=postgres",
			"DB_HOST=db",
		},
		MountPath:    "/var/lib/postgresql/data",
		AppMountPath: "",
		Healthcheck: &container.HealthConfig{
			Test:     []string{"CMD-SHELL", "pg_isready -U postgres"},
			Interval: 2 * time.Second,
			Timeout:  5 * time.Second,
			Retries:  30,
		},
	},
}

func GetDBConfig(dbType DBType) DBConfig {
	cfg, ok := dbConfigs[dbType]
	if !ok {
		panic("unknown DBType: " + string(dbType))
	}
	return cfg
}
