package network

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	Start() error
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	Handler() http.Handler
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send(m []Message)
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	//SendSnapshot(m snap.Message)
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	AddRemote(id ID, urls []string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(id ID, urls []string)
	// RemovePeer removes the peer with given id.
	RemovePeer(id ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	UpdatePeer(id ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	Stop()
}

// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
type Transport struct {
	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo TLSInfo // TLS information used when creating connection

	ID        ID        // local member ID
	URLs      URLs      // local peer URLs
	ClusterID ID        // raft cluster ID for request validation
	Processor Processor // raft state machine, to which the Transport forwards received messages and reports status

	// used to record transportation statistics with followers when
	// performing as leader in raft protocol

	//LeaderStats *stats.LeaderStats

	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams
	pipelineRt http.RoundTripper // roundTripper used by pipelines

	mu      sync.RWMutex   // protect the remote and peer map
	remotes map[ID]*remote // remotes map that helps newly joined member to catch up
	peers   map[ID]Peer    // peers map

}

func (t *Transport) Start() error {
	var err error
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return err
	}
	t.remotes = make(map[ID]*remote)
	t.peers = make(map[ID]Peer)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}

func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.Processor, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Processor, t.ID, t.ClusterID)
	mux := http.NewServeMux()
	mux.Handle(ServerPrefix, pipelineHandler)
	mux.Handle(StreamPrefix+"/", streamHandler)

	return mux
}

func (t *Transport) Get(id ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

func (t *Transport) Send(msgs []Message) {
	for _, m := range msgs {
		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]
		g, rok := t.remotes[to]
		t.mu.RUnlock()

		if pok {
			p.send(m)
			continue
		}

		if rok {
			g.send(m)
			continue
		}

		logger.Debugf("ignored message %s (sent to unknown peer %s)", m.Type, to)
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}

	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

func (t *Transport) AddRemote(id ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := NewURLs(us)
	if err != nil {
		logger.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.remotes[id] = startRemote(t, urls, id)
}

func (t *Transport) AddPeer(id ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		panic("transport stopped")
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	urls, err := NewURLs(us)
	if err != nil {
		logger.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	//fs := t.LeaderStats.Follower(id.String())
	t.peers[id] = startPeer(t, urls, id)

	//addPeerToProber(t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rtts)
	//addPeerToProber(t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rtts)

}

func (t *Transport) RemovePeer(id ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(id ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop()
	} else {
		logger.Panicf("unexpected removal of unknown peer '%d'", id)
	}
	delete(t.peers, id)
	//delete(t.LeaderStats.Followers, id.String())

	logger.Infof("removed peer %s", id)
}

func (t *Transport) UpdatePeer(id ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := NewURLs(us)
	if err != nil {
		logger.Panicf("newURLs %+v should never fail: %+v", us, err)
	}
	t.peers[id].update(urls)

	logger.Infof("updated peer %s", id)
}

func (t *Transport) ActiveSince(id ID) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}

func (t *Transport) DeactivePeers() []int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	buff := make([]int, 0, 0)
	for id, p := range t.peers {
		if p.activeSince().IsZero() {
			buff = append(buff, int(id))
		}
	}
	return buff
}

func (t *Transport) IsPeerActive(peerId int) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	id := ID(peerId)
	peer := t.peers[id]
	return peer.getStatus().isActive()
}
