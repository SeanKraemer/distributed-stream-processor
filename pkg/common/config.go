package common

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the active configuration for a cluster node.
type Config struct {
	NodePort       int      `json:"node_port"`
	ClientPort     int      `json:"client_port"`
	MembershipPort int      `json:"membership_port"`
	RainStormPort  int      `json:"rainstorm_port"`
	Nodes          []string `json:"-"` // Flattened from root
	VMs            []string `json:"-"` // Alias for Nodes (backward compat with internal usages)
}

// rawConfig matches the JSON file structure
type rawConfig struct {
	ActiveProfile string            `json:"active_profile"`
	Profiles      map[string]Config `json:"profiles"`
	Nodes         []string          `json:"nodes"`
}

// LoadConfig reads the JSON configuration and selects the appropriate profile.
// The profile is selected via the RAINSTORM_PROFILE env var, falling back to active_profile.
func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer f.Close()

	var raw rawConfig
	dec := json.NewDecoder(f)
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode config %q: %w", path, err)
	}

	// Determine profile: env var takes precedence, then active_profile
	profileName := os.Getenv("RAINSTORM_PROFILE")
	if profileName == "" {
		profileName = raw.ActiveProfile
	}
	if profileName == "" {
		profileName = "default"
	}

	cfg, ok := raw.Profiles[profileName]
	if !ok {
		return nil, fmt.Errorf("profile %q not found in config", profileName)
	}

	// Inject shared node list (VMs is kept as alias for backward compatibility)
	cfg.Nodes = raw.Nodes
	cfg.VMs = raw.Nodes

	return &cfg, nil
}
