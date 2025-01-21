package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"net"
	"log"
	"os"

	"github.com/Syn3rman/kvstore/pkg/config"
	"github.com/Syn3rman/kvstore/pkg/consistenthash"

	pb "github.com/Syn3rman/kvstore/pkg/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

const (
	MinTimeout = 150
	MaxTimeout = 300
)

type Node struct {
	pb.UnimplementedRaftServiceServer

	config     *config.Config
	nodeConfig *config.NodeConfig

	// Leader election - from Raft
	term     int32
	votedFor string
	leader   string
	state    NodeState

	// Cluster State
	nodes    map[string]config.NodeConfig
	hashRing *consistenthash.HashRing

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Synchronization
	mu     sync.RWMutex
	voteMu sync.Mutex

	joinCh      chan *pb.JoinRequest
	heartbeatCh chan *pb.HeartbeatRequest
	voteCh      chan *pb.VoteRequest
	responsesCh chan interface{}

	logger *log.Logger
}

func randomTimeout(minTimeout int, maxTimeout int) time.Duration {
	return time.Millisecond * time.Duration(
		rand.Intn(maxTimeout-minTimeout)+minTimeout,
	)
}

func NewNode(cfg *config.Config) *Node {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", cfg.NodeID), log.LstdFlags|log.Lmsgprefix)

	n := &Node{
		UnimplementedRaftServiceServer: pb.UnimplementedRaftServiceServer{},
		config:         				cfg,
		nodeConfig:     				cfg.NodeConfig,
		state:          				Follower,
		term: 		 					0,
		nodes:          				make(map[string]config.NodeConfig),
		hashRing:       				consistenthash.NewHashRing(cfg.NumVNodes, nil),
		electionTimer:  				time.NewTimer(time.Duration(cfg.ElectionTimeout)*time.Millisecond),
		heartbeatTimer: 				time.NewTimer(time.Duration(cfg.HeartbeatTimeout) * time.Millisecond),
		joinCh:         				make(chan *pb.JoinRequest, 100),
		heartbeatCh:    				make(chan *pb.HeartbeatRequest, 100),
		voteCh:         				make(chan *pb.VoteRequest, 100),
		responsesCh:    				make(chan interface{}, 10),
		logger:							logger,
	}

	if cfg.IsLeader {
		n.state = Leader
		n.term++;
		n.leader = cfg.NodeConfig.NodeID
		n.logger.Printf("Node %s is starting as Leader for term %d", cfg.NodeID, n.term)
		n.electionTimer.Stop()

		n.hashRing.AddNode(*n.nodeConfig)
		n.nodes[cfg.NodeConfig.NodeID] = *cfg.NodeConfig
	}
	go n.run()
	return n
}

func (n *Node) run() {
	for {
		n.logger.Printf("Current state: %v", n.state)
		switch n.state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) runFollower() {
	for {
		select {
			// TO-DO: if join request, redirect it to leader with your term
			case voteReq := <-n.voteCh:
				n.handleVoteRequest(voteReq)
			case heartbeat := <-n.heartbeatCh:
				n.handleHeartbeat(heartbeat)
			case <-n.electionTimer.C:
				n.logger.Printf("Election timer fired. Transitioning to Candidate.")
				// remove leader from hash ring and become candidate
				n.removeNode(n.leader, "no heartbeat from leader")
				n.leader = ""
				n.becomeCandidate()
				return
			}
	}
}

func (n *Node) runCandidate() {
	n.logger.Printf("Running in Candidate state.")
	// if only one node, become leader
	if len(n.nodes) == 1 {
		n.logger.Printf("Only one node in the cluster. Becoming Leader.")
		n.becomeLeader()
		return
	}

	// Prepare vote request
	voteRequest := &pb.VoteRequest{
		Term:        n.term,
		CandidateId: n.nodeConfig.NodeID,
	}

	done := make(chan bool, 1)
	go n.broadcastVoteRequests(voteRequest, done)

	n.handleElectionResult(done)
}

func (n *Node) runLeader() {

	for {
		select {
		case heartbeat := <-n.heartbeatCh:
			n.handleHeartbeat(heartbeat)
			if n.state == Follower {
				n.logger.Printf("Received valid heartbeat from another leader. Stepping down to Follower.")
				return
			}
		case <-n.heartbeatTimer.C:
			n.sendHeartbeats()
		case joinReq := <-n.joinCh:
			n.HandleJoinRequest(context.Background(), joinReq)
		case <-n.electionTimer.C:
			// If election timer fires, it indicates a leadership issue
			n.logger.Printf("Election timer fired unexpectedly in leader state. Stepping down to Follower.")
			n.becomeFollower("election timer triggered")
			return
		}
	}
}

func (n *Node) StartServer() error {
    lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", n.nodeConfig.Host, n.nodeConfig.Port))
    if err != nil {
        return fmt.Errorf("failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterRaftServiceServer(grpcServer, n)

    go func() {
        if err := grpcServer.Serve(lis); err != nil {
            log.Fatalf("failed to serve: %v", err)
        }
    }()

    return nil
}

func (n *Node) resetElectionTimer(electionTimeout int) {
	n.electionTimer.Reset(randomTimeout(electionTimeout, electionTimeout*2))
}

func (n *Node) broadcastVoteRequests(voteRequest *pb.VoteRequest, done chan bool) {
	var once sync.Once

	votesReceived := 1 // Vote for self
	totalNodes := len(n.nodes)
	majorityRequired := (totalNodes / 2) + 1

	n.logger.Printf("Broadcasting vote requests for term %d. Majority required: %d", voteRequest.Term, majorityRequired)

	for nodeID, nodeConfig := range n.nodes {
		// Skip self
		if nodeID == n.nodeConfig.NodeID {
			continue
		}

		go func(nodeConfig config.NodeConfig) {
			resp, err := n.requestVoteFromNode(nodeConfig, voteRequest)
			if err != nil {
				return
			}

			n.voteMu.Lock()
			defer n.voteMu.Unlock()

			// If response term is higher, step down to follower
			if resp.Term > n.term {
				n.becomeFollower("higher term detected")
				once.Do(func() { done <- false })
				return
			}

			if resp.VoteGranted {
				n.logger.Printf("Vote granted by node %s. Total votes: %d", nodeConfig.NodeID, votesReceived)
				votesReceived++
				if votesReceived >= majorityRequired {
					once.Do(func() { done <- true })
				}
			}
		}(nodeConfig)
	}
	go func() {
		time.Sleep(time.Duration(n.config.ElectionTimeout) * time.Millisecond)
		n.voteMu.Lock()
		defer n.voteMu.Unlock()

		if votesReceived < majorityRequired {
			n.logger.Printf("Election timed out for term %d. Reverting to Follower.", voteRequest.Term)
			once.Do(func() { done <- false }) // Signal failure due to timeout
		}
	}()
}

func (n *Node) handleElectionResult(done chan bool) {
	for {
		select {
		case result := <-done:
			if result {
				n.logger.Printf("Election successful. Transitioning to Leader.")
				n.becomeLeader()
			} else {
				n.becomeFollower("did not receive majority votes")
			}
			return
		case voteReq := <-n.voteCh:
			response := n.handleVoteRequest(voteReq)
			if response.Term > n.term {
				n.logger.Printf("Received vote request with higher term %d. Stepping down to Follower.", response.Term)
				n.becomeFollower("higher term detected")
				return
			}
		case heartbeat := <-n.heartbeatCh:
			n.handleHeartbeat(heartbeat)
			if n.state == Follower {
				n.logger.Printf("Received valid heartbeat. Stepping down to Follower.")
				n.becomeFollower("received heartbeat")
				return
			}
		case <-n.electionTimer.C:
			n.logger.Printf("Election timeout. Reverting to Follower.")
			n.becomeFollower("election timeout")
			return
		}
	}
}

func (n *Node) becomeFollower(reason string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("Transitioning to Follower state. Reason: %s", reason)

	n.state = Follower
	n.votedFor = ""
	if reason == "higher term detected" {
		n.leader = ""
	}
	n.resetElectionTimer(n.config.ElectionTimeout)
}


func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate
	n.term++
	n.votedFor = n.nodeConfig.NodeID

	n.resetElectionTimer(n.config.ElectionTimeout)
}

func (n *Node) requestVoteFromNode(nodeConfig config.NodeConfig, voteRequest *pb.VoteRequest) (*pb.VoteResponse, error) {
	address := fmt.Sprintf("%s:%d", nodeConfig.Host, nodeConfig.Port)
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		n.logger.Printf("Failed to connect to node %s (%s): %v", nodeConfig.NodeID, address, err)
		return nil, fmt.Errorf("failed to connect to node %s: %v", nodeConfig.NodeID, err)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n.logger.Printf("Sending vote request to node %s (%s) for term %d", nodeConfig.NodeID, address, voteRequest.Term)
	resp, err := client.RequestVote(ctx, voteRequest)
	if err != nil {
		n.logger.Printf("Vote request to node %s (%s) failed: %v", nodeConfig.NodeID, address, err)
		return nil, fmt.Errorf("vote request to node %s failed: %v", nodeConfig.NodeID, err)
	}

	n.logger.Printf("Vote request to node %s succeeded: vote granted = %t, term = %d", nodeConfig.NodeID, resp.VoteGranted, resp.Term)
	return resp, nil
}

func (n *Node) RequestVote(ctx context.Context, voteRequest *pb.VoteRequest) (*pb.VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	voteResponse := &pb.VoteResponse{
		Term:        n.term,
		VoteGranted: false,
	}

	if voteRequest.Term < n.term {
		n.logger.Printf("Rejected vote request from %s (term: %d < current term: %d)", voteRequest.CandidateId, voteRequest.Term, n.term)
		return voteResponse, nil
	}

	if voteRequest.Term > n.term {
		n.term = voteRequest.Term
		n.becomeFollower("higher term detected")
	}

	if n.votedFor == "" || n.votedFor == voteRequest.CandidateId {
		voteResponse.VoteGranted = true
		n.votedFor = voteRequest.CandidateId
		n.resetElectionTimer(n.config.ElectionTimeout)
		n.logger.Printf("Granted vote to %s for term %d", voteRequest.CandidateId, voteRequest.Term)
	} else {
		n.logger.Printf("Rejected vote request from %s (already voted for %s in term %d)", voteRequest.CandidateId, n.votedFor, n.term)
	}

	voteResponse.Term = n.term
	return voteResponse, nil
}

func (n *Node) handleVoteRequest(req *pb.VoteRequest) *pb.VoteResponse {
	n.voteMu.Lock()
	defer n.voteMu.Unlock()

	// Default response
	resp := &pb.VoteResponse{
		Term:        n.term,
		VoteGranted: false,
	}

	// Check if request term is valid
	if req.Term < n.term {
		n.logger.Printf("Rejected vote request from %s (term: %d < current term: %d)", req.CandidateId, req.Term, n.term)
		return resp
	}

	// If request term is higher, update our term
	if req.Term > n.term {
		n.logger.Printf("Updated term to %d due to vote request from %s", req.Term, req.CandidateId)
		n.term = req.Term
		n.votedFor = ""
		n.state = Follower
	}

	// Check if we haven't voted or voted for this candidate in this term
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		resp.VoteGranted = true
		n.votedFor = req.CandidateId
		n.resetElectionTimer(n.config.ElectionTimeout)
		n.logger.Printf("Granted vote to candidate %s for term %d", req.CandidateId, req.Term)
	} else {
		n.logger.Printf("Rejected vote request from candidate %s (already voted for %s in term %d)", req.CandidateId, n.votedFor, n.term)
	}

	resp.Term = n.term
	return resp
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Leader
	n.leader = n.nodeConfig.NodeID
	n.electionTimer.Stop()
	n.logger.Printf("Node %s has become the Leader for term %d.", n.nodeConfig.NodeID, n.term)
}

func (n *Node) handleHeartbeat(heartbeat *pb.HeartbeatRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("Received heartbeat from leader %s for term %d", heartbeat.LeaderId, heartbeat.Term)
	response := &pb.HeartbeatResponse{Success: false}
	defer func() {
        n.responsesCh <- response
    }()

	if heartbeat.Term < n.term {
		n.logger.Printf("Ignored heartbeat from %s (term: %d < current term: %d)", heartbeat.LeaderId, heartbeat.Term, n.term)
		return
	}

	if heartbeat.Term > n.term+1 {
		n.logger.Printf("Updating term to %d due to heartbeat from %s", heartbeat.Term, heartbeat.LeaderId)
		n.term = heartbeat.Term
		n.becomeFollower("higher term detected")
	}

	// Accept heartbeat if:
    // 1. It is from current leader, or
    // 2. We don't have a leader yet, or
    // 3. It is the same term and we're accepting a new leader
    if heartbeat.Term >= n.term &&
        (heartbeat.LeaderId == n.leader || n.leader == "") {

        if n.leader == "" {
            n.logger.Printf("Accepting %s as the new leader for term %d",
                heartbeat.LeaderId, heartbeat.Term)
        }

        n.leader = heartbeat.LeaderId
        go n.updateHashRing(heartbeat.Nodes)
        n.resetElectionTimer(n.config.ElectionTimeout)

        n.logger.Printf("Heartbeat from leader %s accepted. Resetting election timer.",
            heartbeat.LeaderId)

        response.Success = true
        return
    }

    n.logger.Printf("Ignored heartbeat from unrecognized leader %s (current leader: %s)",
        heartbeat.LeaderId, n.leader)
}

func (n *Node) SendJoinRequest(leader config.NodeConfig) error {
	conn, err := grpc.NewClient(
		fmt.Sprintf("%s:%d", leader.Host, leader.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to leader %s: %v", leader.NodeID, err)
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)

	req := &pb.JoinRequest{
		NodeId: n.nodeConfig.NodeID,
		Host:   n.nodeConfig.Host,
		Port:   int32(n.nodeConfig.Port),
	}

	resp, err := client.JoinCluster(context.Background(), req)
	if err != nil {
		return fmt.Errorf("join request failed: %v", err)
	}

	// Process the response
	if !resp.Success {
		return nil
	}

	// Update local cluster state with the response
	for nodeID, nodeConfig := range resp.ClusterNodes {
		n.nodes[nodeID] = config.NodeConfig{
			NodeID: nodeConfig.NodeId,
			Host:   nodeConfig.Host,
			Port:   int(nodeConfig.Port),
		}
	}
	return nil
}

func (n *Node) JoinCluster(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
    n.logger.Printf("Received join request from node %s at %s:%d", req.NodeId, req.Host, req.Port)

    n.logger.Printf("Forwarding join request to internal channel")
    n.joinCh <- req

    select {
    case resp := <-n.responsesCh:
        if joinResp, ok := resp.(*pb.JoinResponse); ok {
            if joinResp.Success {
                n.logger.Printf("Join request from node %s successful. Cluster now has %d nodes",
                    req.NodeId, len(joinResp.ClusterNodes))
            } else {
                n.logger.Printf("Join request from node %s unsuccessful", req.NodeId)
            }
            return joinResp, nil
        }
        n.logger.Printf("Error: received invalid response type for join request from node %s", req.NodeId)
        return &pb.JoinResponse{Success: false}, fmt.Errorf("invalid response type")

    case <-ctx.Done():
        n.logger.Printf("Join request from node %s timed out or cancelled", req.NodeId)
        return &pb.JoinResponse{Success: false}, ctx.Err()
    }
}

func (n *Node) HandleJoinRequest(ctx context.Context, req *pb.JoinRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Printf("Processing join request from node %s at %s:%d", req.NodeId, req.Host, req.Port)

	response := &pb.JoinResponse{
        Success: false,
        ClusterNodes: n.getClusterNodesAsMap(),
    }
	defer func() {
		n.logger.Printf("Sending join response (success=%v) to node %s", response.Success, req.NodeId)
        n.responsesCh <- response
    }()

	if n.state != Leader {
		n.logger.Printf("Rejecting join request from %s: not leader", req.NodeId)
		return
	}

	if _, exists := n.nodes[req.NodeId]; exists {
        n.logger.Printf("Node %s already exists in cluster", req.NodeId)
        response.Success = true // Consider this a success since node is already part of cluster
        return
    }

	// Add the new node to the cluster
	newNode := config.NodeConfig{
		NodeID: req.NodeId,
		Host:   req.Host,
		Port:   int(req.Port),
	}
	n.hashRing.AddNode(newNode)
	n.nodes[req.NodeId] = newNode

	n.broadcastClusterUpdate()
    n.logger.Printf("Successfully added node %s to cluster", req.NodeId)
    response.Success = true
}

func (n *Node) broadcastClusterUpdate() {
	go n.sendHeartbeats()
}

// Helper method to convert cluster nodes to a map for the response
func (n *Node) getClusterNodesAsMap() map[string]*pb.NodeConfig {
	clusterNodes := make(map[string]*pb.NodeConfig)
	for nodeID, config := range n.nodes {
		clusterNodes[nodeID] = &pb.NodeConfig{
			NodeId: config.NodeID,
			Host:   config.Host,
			Port:   int32(config.Port),
		}
	}
	return clusterNodes
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Prepare protobuf nodes map
	protoNodes := make(map[string]*pb.NodeConfig)
	for nodeID, nodeCfg := range n.nodes {
		protoNodes[nodeID] = &pb.NodeConfig{
			NodeId: nodeCfg.NodeID,
			Host:   nodeCfg.Host,
			Port:   int32(nodeCfg.Port),
		}
	}

	heartbeat := &pb.HeartbeatRequest{
		Term:     n.term,
		LeaderId: n.nodeConfig.NodeID,
		Nodes:    protoNodes,
	}

	go func() {
		totalNodes := len(n.nodes) - 1 // Excluding self
        if totalNodes == 0 {
            return
        }
		for nodeID, nodeConfig := range n.nodes {
			// Skip sending heartbeat to self
			if nodeID == n.nodeConfig.NodeID {
				continue
			}

			go func(nodeID string, nodeConfig config.NodeConfig) {
				n.logger.Printf("Attempting heartbeat to node %s at %s:%d",
                    nodeID, nodeConfig.Host, nodeConfig.Port)

				conn, err := grpc.NewClient(
					fmt.Sprintf("%s:%d", nodeConfig.Host, nodeConfig.Port),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					n.logger.Printf("Failed to connect to node %s (%s:%d): %v", nodeID, nodeConfig.Host, nodeConfig.Port, err)
					return
				}
				defer conn.Close()

				client := pb.NewRaftServiceClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				resp, err := client.SendHeartbeat(ctx, heartbeat)
				if err != nil {
					n.removeNode(nodeID, "heartbeat failure")
					return
				}

				if !resp.Success {
					n.removeNode(nodeID, "unsuccessful heartbeat response")
				} else {
					n.logger.Printf("Heartbeat to node %s successful", nodeID)
				}
			}(nodeID, nodeConfig)
		}
	}()

	// Reset heartbeat timer
	n.heartbeatTimer.Reset(time.Duration(n.config.HeartbeatTimeout) * time.Millisecond)
}

func (n *Node) removeNode(nodeID, reason string) {
    n.mu.Lock()
    defer n.mu.Unlock()

    n.logger.Printf("Removing node %s from cluster due to: %s", nodeID, reason)
    n.hashRing.RemoveNode(nodeID)
    delete(n.nodes, nodeID)
}

func (n *Node) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	n.heartbeatCh <- req

	select {
    case resp := <-n.responsesCh:
        if heartbeatResp, ok := resp.(*pb.HeartbeatResponse); ok {
			n.logger.Printf("Received heartbeat response: %v", heartbeatResp)
			n.mu.Lock()
			n.resetElectionTimer(n.config.ElectionTimeout)
			n.mu.Unlock()
            return heartbeatResp, nil
        }
        return &pb.HeartbeatResponse{Success: false}, fmt.Errorf("invalid response type")
    case <-ctx.Done():
        return &pb.HeartbeatResponse{Success: false}, ctx.Err()
    }
}


func (n *Node) updateHashRing(nodes map[string]*pb.NodeConfig) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.hashRing.Clear()
	n.nodes = make(map[string]config.NodeConfig)

	for nodeID, nodeConfig := range nodes {
		if nodeID == "" || nodeConfig.Host == "" || nodeConfig.Port == 0 {
			n.logger.Printf("Skipping invalid node: %s", nodeID)
			continue
		}

		newNode := config.NodeConfig{
			NodeID: nodeConfig.NodeId,
			Host:   nodeConfig.Host,
			Port:   int(nodeConfig.Port),
		}

		n.hashRing.AddNode(newNode)
		n.nodes[nodeID] = newNode
	}
	n.logger.Printf("Hash ring updated successfully with %d nodes", len(n.nodes))
}
