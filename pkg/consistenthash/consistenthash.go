package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/Syn3rman/kvstore/pkg/config"
)

type HashFunc func(data string) uint64

type HashRing struct {
    ring			map[uint64]string // hash -> nodeID
    nodes			map[string]config.NodeConfig
	hashFunc 		HashFunc
    virtualNodes	int
    mu				sync.RWMutex
}

func defaultHashFunc(data string) uint64 {
    return uint64(crc32.ChecksumIEEE([]byte(data)))
}

func NewHashRing(virtualNodes int, hashFunc HashFunc) *HashRing {
	if hashFunc == nil {
		hashFunc = defaultHashFunc
	}

    return &HashRing{
        ring:			make(map[uint64]string),
        nodes:			make(map[string]config.NodeConfig),
        virtualNodes:	virtualNodes,
		hashFunc:		hashFunc,
    }
}

func (hr *HashRing) AddNode(config config.NodeConfig) {
    hr.mu.Lock()
    defer hr.mu.Unlock()

    hr.nodes[config.NodeID] = config
    for i := 0; i < hr.virtualNodes; i++ {
        hash := hr.hashFunc(fmt.Sprintf("%s:%d", config.NodeID, i))
        hr.ring[hash] = config.NodeID
    }
}

func (hr *HashRing) RemoveNode(nodeID string) {
    hr.mu.Lock()
    defer hr.mu.Unlock()

    delete(hr.nodes, nodeID)
    for hash, node := range hr.ring {
        if node == nodeID {
            delete(hr.ring, hash)
        }
    }
}

func (hr *HashRing) Clear() {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.ring = make(map[uint64]string)
	hr.nodes = make(map[string]config.NodeConfig)
}