package bully

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

type Peer struct {
	ID   int
	Addr string
}

type Config struct {
	SelfID       int
	Peers        []Peer
	ListenAddr   string
	PingInterval time.Duration
	PingTimeout  time.Duration
}

type peerState struct {
	id          int
	addr        string
	alive       bool
	missedPings int
}

type Bully struct {
	cfg      Config
	peers    []*peerState
	selfID   int
	leaderID int
	isLeader bool
	listener net.Listener
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	log      *slog.Logger
}

func New(cfg Config) *Bully {
	b := &Bully{
		cfg:      cfg,
		selfID:   cfg.SelfID,
		leaderID: 0,
		isLeader: cfg.SelfID == 0,
		log:      slog.With("component", "Bully"),
	}
	for _, p := range cfg.Peers {
		b.peers = append(b.peers, &peerState{id: p.ID, addr: p.Addr, alive: true})
	}
	return b
}

func (b *Bully) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	b.ctx = ctx
	b.cancel = cancel

	listener, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("bully: listen %s: %w", b.cfg.ListenAddr, err)
	}
	b.listener = listener

	b.log.Info("bully started", "selfID", b.selfID, "peers", len(b.peers), "listenAddr", b.cfg.ListenAddr, "leader", b.isLeader)
	go b.acceptLoop()
	go b.pingLoop()

	return nil
}

func (b *Bully) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	if b.listener != nil {
		b.listener.Close()
	}
}

func (b *Bully) LeaderID() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.leaderID
}

func (b *Bully) IsLeader() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isLeader
}

func (b *Bully) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if b.ctx.Err() != nil {
				return
			}
			b.log.Warn("accept error", "error", err)
			continue
		}
		go b.handleConn(conn)
	}
}

func (b *Bully) handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 3)
	conn.SetDeadline(time.Now().Add(b.cfg.PingTimeout * 2))
	n, err := conn.Read(buf)
	if err != nil || n < 3 {
		return
	}

	msg, err := Decode(buf)
	if err != nil {
		b.log.Warn("invalid message", "error", err)
		return
	}
	
	switch msg.Type {
	case MsgPing:
		b.handlePing(conn, msg)
	case MsgElection:
		b.handleElection(conn, msg)
	case MsgCoordinator:
		b.handleCoordinator(msg)
	case MsgAlive:
		b.handleAlive(conn, msg)
	}
}

func (b *Bully) handlePing(conn net.Conn, msg Message) {
	b.mu.Lock()
	for _, p := range b.peers {
		if p.id == msg.SenderID {
			p.alive = true
			p.missedPings = 0
			break
		}
	}
	leaderID := b.leaderID
	b.mu.Unlock()

	if msg.LeaderID >= 0 && msg.LeaderID != leaderID {
		b.log.Info("conflict: ping claims other leader, starting election",
			"claimedLeader", msg.LeaderID, "currentLeader", leaderID, "sender", msg.SenderID)
		go b.startElection()
	}

	resp := Encode(Message{Type: MsgPong, SenderID: b.selfID, LeaderID: leaderID})
	conn.Write(resp)
}

func (b *Bully) handleElection(conn net.Conn, msg Message) {
	if msg.SenderID > b.selfID {
		resp := Encode(Message{Type: MsgAlive, SenderID: b.selfID, LeaderID: b.leaderID})
		conn.Write(resp)
		go b.startElection()
	}
}

func (b *Bully) handleCoordinator(msg Message) {
	b.mu.Lock()
	b.leaderID = msg.LeaderID
	b.isLeader = (msg.LeaderID == b.selfID)
	b.mu.Unlock()

	b.log.Info("coordinator received", "leaderID", msg.LeaderID, "isSelf", b.isLeader)
}

func (b *Bully) handleAlive(conn net.Conn, msg Message) {
	b.mu.Lock()
	for _, p := range b.peers {
		if p.id == msg.SenderID {
			p.alive = true
			p.missedPings = 0
			break
		}
	}
	leaderID := b.leaderID
	b.mu.Unlock()

	resp := Encode(Message{Type: MsgCoordinator, SenderID: b.selfID, LeaderID: leaderID})
	conn.Write(resp)
}

func (b *Bully) pingLoop() {
	ticker := time.NewTicker(b.cfg.PingInterval)
	defer ticker.Stop()

	time.Sleep(500 * time.Millisecond)

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.pingPeers()
		}
	}
}

func (b *Bully) pingPeers() bool {
	anyAlive := false
	var wg sync.WaitGroup

	for _, peer := range b.peers {
		wg.Add(1)
		go func(p *peerState) {
			defer wg.Done()
			if b.pingPeer(p) {
				anyAlive = true
			}
		}(peer)
	}
	wg.Wait()

	return anyAlive
}

func (b *Bully) pingPeer(peer *peerState) bool {
	conn, err := net.DialTimeout("tcp", peer.addr, b.cfg.PingTimeout)
	if err != nil {
		b.markPeerDead(peer.id)
		return false
	}
	defer conn.Close()

	b.mu.RLock()
	leaderID := b.leaderID
	b.mu.RUnlock()

	msg := Encode(Message{Type: MsgPing, SenderID: b.selfID, LeaderID: leaderID})
	if _, err := conn.Write(msg); err != nil {
		b.markPeerDead(peer.id)
		return false
	}

	conn.SetReadDeadline(time.Now().Add(b.cfg.PingTimeout))
	buf := make([]byte, 3)
	n, err := conn.Read(buf)
	if err != nil || n < 3 {
		b.markPeerDead(peer.id)
		return false
	}

	resp, err := Decode(buf)
	if err != nil || resp.Type != MsgPong {
		b.markPeerDead(peer.id)
		return false
	}

	b.handlePong(peer.id, resp)
	return true
}

func (b *Bully) handlePong(peerID int, _ Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range b.peers {
		if p.id == peerID {
			p.alive = true
			p.missedPings = 0
			break
		}
	}
}

func (b *Bully) markPeerDead(peerID int) {
	b.mu.Lock()

	var peer *peerState
	for _, p := range b.peers {
		if p.id == peerID {
			peer = p
			break
		}
	}
	if peer == nil {
		b.mu.Unlock()
		return
	}

	peer.missedPings++
	shouldElect := false
	if peer.missedPings >= 3 && peer.alive {
		peer.alive = false
		b.log.Warn("peer dead", "peerID", peerID, "missedPings", peer.missedPings)
		shouldElect = (peerID == b.leaderID)
	}
	b.mu.Unlock()

	if shouldElect {
		b.startElection()
	}
}

func (b *Bully) startElection() {
	b.log.Info("starting election", "selfID", b.selfID)

	b.mu.Lock()
	var candidates []*peerState
	for _, p := range b.peers {
		if p.id < b.selfID && p.alive {
			candidates = append(candidates, p)
		}
	}
	b.mu.Unlock()

	for _, c := range candidates {
		if b.sendElection(c) {
			b.log.Info("election deferred to lower-ID peer", "peerID", c.id)
			return
		}
	}

	b.becomeLeader()
}

func (b *Bully) sendElection(peer *peerState) bool {
	conn, err := net.DialTimeout("tcp", peer.addr, b.cfg.PingTimeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	b.mu.RLock()
	leaderID := b.leaderID
	b.mu.RUnlock()

	msg := Encode(Message{Type: MsgElection, SenderID: b.selfID, LeaderID: leaderID})
	if _, err := conn.Write(msg); err != nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(b.cfg.PingTimeout))
	buf := make([]byte, 3)
	n, err := conn.Read(buf)
	if err != nil || n < 3 {
		return false
	}

	resp, _ := Decode(buf)
	return resp.Type == MsgAlive
}

func (b *Bully) becomeLeader() {
	b.mu.Lock()
	b.leaderID = b.selfID
	b.isLeader = true
	b.mu.Unlock()

	b.log.Info("became leader", "selfID", b.selfID)

	msg := Encode(Message{Type: MsgCoordinator, SenderID: b.selfID, LeaderID: b.selfID})
	for _, p := range b.peers {
		go b.sendToPeer(p, msg)
	}
}

func (b *Bully) sendToPeer(peer *peerState, msg []byte) {
	conn, err := net.DialTimeout("tcp", peer.addr, b.cfg.PingTimeout)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(msg)
}
