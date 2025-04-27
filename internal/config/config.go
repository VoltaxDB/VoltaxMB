package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Host           string `mapstructure:"host"`
	Port           int    `mapstructure:"port"`
	MaxConnections int    `mapstructure:"max_connections"`
	ReadTimeout    int    `mapstructure:"read_timeout"`
	WriteTimeout   int    `mapstructure:"write_timeout"`

	DataDir          string `mapstructure:"data_dir"`
	EnableAOF        bool   `mapstructure:"enable_aof"`
	AOFSyncInterval  int    `mapstructure:"aof_sync_interval"`
	SnapshotInterval int    `mapstructure:"snapshot_interval"`

	MaxMessageSize int `mapstructure:"max_message_size"`
	RetentionTime  int `mapstructure:"retention_time"`
	MaxPartitions  int `mapstructure:"max_partitions"`
	ReplicaFactor  int `mapstructure:"replica_factor"`

	EnableAuth  bool   `mapstructure:"enable_auth"`
	JWTSecret   string `mapstructure:"jwt_secret"`
	TLSEnabled  bool   `mapstructure:"tls_enabled"`
	TLSCertFile string `mapstructure:"tls_cert_file"`
	TLSKeyFile  string `mapstructure:"tls_key_file"`

	EnableMetrics bool `mapstructure:"enable_metrics"`
	MetricsPort   int  `mapstructure:"metrics_port"`
}

// LoadConfig loads the configuration from the specified file
func LoadConfig(configFile string) (*Config, error) {
	viper.SetConfigFile(configFile)
	viper.AutomaticEnv()

	// Set default values
	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// setDefaults sets the default configuration values
func setDefaults() {
	viper.SetDefault("host", "0.0.0.0")
	viper.SetDefault("port", 9090)
	viper.SetDefault("max_connections", 1000)
	viper.SetDefault("read_timeout", 30)
	viper.SetDefault("write_timeout", 30)

	viper.SetDefault("data_dir", "./data")
	viper.SetDefault("enable_aof", true)
	viper.SetDefault("aof_sync_interval", 1000)
	viper.SetDefault("snapshot_interval", 3600)

	viper.SetDefault("max_message_size", 1048576) // 1MB
	viper.SetDefault("retention_time", 604800)    // 7 days
	viper.SetDefault("max_partitions", 32)
	viper.SetDefault("replica_factor", 1)

	viper.SetDefault("enable_auth", false)
	viper.SetDefault("jwt_secret", "")
	viper.SetDefault("tls_enabled", false)
	viper.SetDefault("tls_cert_file", "")
	viper.SetDefault("tls_key_file", "")

	viper.SetDefault("enable_metrics", true)
	viper.SetDefault("metrics_port", 9091)
}

// Validate validates the configuration values
func (c *Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port number: %d", c.Port)
	}

	if c.MaxConnections < 1 {
		return fmt.Errorf("invalid max_connections: %d", c.MaxConnections)
	}

	if c.ReadTimeout < 1 {
		return fmt.Errorf("invalid read_timeout: %d", c.ReadTimeout)
	}

	if c.WriteTimeout < 1 {
		return fmt.Errorf("invalid write_timeout: %d", c.WriteTimeout)
	}

	if c.MaxMessageSize < 1 {
		return fmt.Errorf("invalid max_message_size: %d", c.MaxMessageSize)
	}

	if c.RetentionTime < 0 {
		return fmt.Errorf("invalid retention_time: %d", c.RetentionTime)
	}

	if c.MaxPartitions < 1 {
		return fmt.Errorf("invalid max_partitions: %d", c.MaxPartitions)
	}

	if c.ReplicaFactor < 1 {
		return fmt.Errorf("invalid replica_factor: %d", c.ReplicaFactor)
	}

	if c.TLSEnabled {
		if c.TLSCertFile == "" {
			return fmt.Errorf("tls_cert_file is required when TLS is enabled")
		}
		if c.TLSKeyFile == "" {
			return fmt.Errorf("tls_key_file is required when TLS is enabled")
		}
	}

	if c.EnableMetrics && (c.MetricsPort < 1 || c.MetricsPort > 65535) {
		return fmt.Errorf("invalid metrics_port: %d", c.MetricsPort)
	}

	return nil
}
