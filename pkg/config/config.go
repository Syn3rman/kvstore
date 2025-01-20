package config

import (
    "encoding/json"
    "os"
    "fmt"

    pb "github.com/Syn3rman/kvstore/pkg/grpc/proto"
)

type Config struct {
	NodeID            string       `json:"node_id"`
	IsLeader          bool         `json:"is_leader"`
	LeaderIP          string       `json:"leader_ip"`
    LeaderPort        int          `json:"leader_port"`
	NumNodes          int          `json:"num_nodes"`
	NumVNodes         int          `json:"num_vnodes"`
	ElectionTimeout   int          `json:"election_timeout"`
	HeartbeatTimeout  int          `json:"heartbeat_timeout"`
	NodeConfig        *NodeConfig  `json:"node_config"`
}


type NodeConfig struct {
	NodeID string `json:"node_id"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
}

func NodeConfigFromProto(pbConfig *pb.NodeConfig) NodeConfig {
    return NodeConfig{
        NodeID: pbConfig.NodeId,
        Host:   pbConfig.Host,
        Port:   int(pbConfig.Port),
    }
}

func NodeConfigToProto(cfg NodeConfig) *pb.NodeConfig {
    return &pb.NodeConfig{
        NodeId: cfg.NodeID,
        Host:   cfg.Host,
        Port:   int32(cfg.Port),
    }
}

func Load(path string) (*Config, error) {
    // Open the configuration file
    file, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("failed to open config file at %s: %v", path, err)
    }
    defer file.Close()

    // Decode the JSON configuration
    var cfg Config
    if err := json.NewDecoder(file).Decode(&cfg); err != nil {
        return nil, fmt.Errorf("failed to decode config file at %s: %v", path, err)
    }

    // Validate the configuration
    if err := validateConfig(&cfg); err != nil {
        return nil, fmt.Errorf("invalid configuration: %v", err)
    }

    return &cfg, nil
}

func validateConfig(cfg *Config) error {
    // Validate required fields
    if cfg.NodeID == "" {
        return fmt.Errorf("NodeID is required")
    }
    if cfg.LeaderIP == "" && !cfg.IsLeader {
        return fmt.Errorf("LeaderIP is required for non-leader nodes")
    }
    if cfg.NumVNodes <= 0 {
        return fmt.Errorf("NumVNodes must be greater than zero")
    }
    if cfg.ElectionTimeout <= 0 {
        return fmt.Errorf("ElectionTimeout must be greater than zero")
    }
    if cfg.HeartbeatTimeout <= 0 {
        return fmt.Errorf("HeartbeatTimeout must be greater than zero")
    }
    if cfg.NodeConfig == nil {
        return fmt.Errorf("NodeConfig is required")
    }
    if cfg.NodeConfig.NodeID == "" {
        return fmt.Errorf("NodeConfig.NodeID is required")
    }
    if cfg.NodeConfig.Host == "" {
        return fmt.Errorf("NodeConfig.Host is required")
    }
    if cfg.NodeConfig.Port <= 0 {
        return fmt.Errorf("NodeConfig.Port must be greater than zero")
    }

    return nil
}
