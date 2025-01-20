package main

import (
	"flag"
	"fmt"
	"log"
	"github.com/Syn3rman/kvstore/pkg/config"
	"github.com/Syn3rman/kvstore/pkg/raft"
)

func main() {
	configPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()
	cfg, err := config.Load(*configPath)
    if err != nil {
        fmt.Println("Failed to load config:", err)
        return
    }
	nodeConfig := cfg.NodeConfig
	if nodeConfig == nil {
		log.Fatalf("Invalid node configuration")
	}

	// Print node information
	log.Printf("Starting node %s at %s:%d", nodeConfig.NodeID, nodeConfig.Host, nodeConfig.Port)

	// Create and start the node
	node := raft.NewNode(cfg)

	go func() {
		if err := node.StartServer(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	if !cfg.IsLeader {
		leaderConf := config.NodeConfig{
			Host:   cfg.LeaderIP,
			Port:   cfg.LeaderPort,
		}
		log.Printf("Joining leader at %s:%d", leaderConf.Host, leaderConf.Port)
		if err := node.SendJoinRequest(leaderConf); err != nil {
			log.Fatalf("Failed to join cluster: %v", err)
		}
	}
	select {}
}