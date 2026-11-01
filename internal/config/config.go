// Package config is the CLI's settings file: which QueryDesk it signs in to and the password
// local database clients connect with.
package config

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const DefaultServer = "https://app.querydesk.com"

type Config struct {
	Server string `json:"server"`
	// what a database client on this machine connects with, so another local user or
	// process can't run queries as the member
	LocalPassword string `json:"local_password"`
	PostgresPort  int    `json:"postgres_port"`
	MySQLPort     int    `json:"mysql_port"`
}

// Dir is the CLI's own directory, QUERYDESK_CONFIG_DIR or the OS's config directory.
func Dir() (string, error) {
	if dir := os.Getenv("QUERYDESK_CONFIG_DIR"); dir != "" {
		return dir, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "querydesk"), nil
}

// Load reads the config, filling in what's missing and saving it when it did.
func Load() (*Config, error) {
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "config.json")

	var config Config
	raw, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(raw, &config); err != nil {
			return nil, err
		}
	}

	before := config
	if config.Server == "" {
		config.Server = DefaultServer
	}
	if config.LocalPassword == "" {
		b := make([]byte, 18)
		_, _ = rand.Read(b)
		config.LocalPassword = base64.RawURLEncoding.EncodeToString(b)
	}
	// clear of the databases' own ports, so a local Postgres or MySQL can keep running
	if config.PostgresPort == 0 {
		config.PostgresPort = 15432
	}
	if config.MySQLPort == 0 {
		config.MySQLPort = 13306
	}
	if config != before {
		if err := config.Save(); err != nil {
			return nil, err
		}
	}
	return &config, nil
}

func (c *Config) Save() error {
	dir, err := Dir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(filepath.Join(dir, "config.json"), append(raw, '\n'), 0o600)
}
