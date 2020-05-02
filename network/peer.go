package network

import (
	"context"
	"github.com/xp/shorttext-db/glogger"
	"sync"
	"time"
)

var logger = glogger.MustGetLogger("network")

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	send(m Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	//sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	update(urls URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	stop()

	getStatus() *peerStatus
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	// id of the remote raft peer node
	id        ID
	processor Processor

	status *peerStatus

	picker *urlPicker

	writer   *streamWriter
	pipeline *pipeline
	reader   *streamReader

	recvc chan Message
	propc chan Message
	//用于处理返回值
	resultc chan Message
	mu      sync.Mutex
	paused  bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(transport *Transport, urls URLs, peerID ID) *peer {

	logger.Infof("started peer [%d]", peerID)

	status := newPeerStatus(peerID)
	picker := newURLPicker(urls)
	errorc := transport.ErrorC
	r := transport.Processor
	pipeline := &pipeline{
		peerID:    peerID,
		tr:        transport,
		picker:    picker,
		status:    status,
		processor: r,
		errorc:    errorc,
	}
	pipeline.start()

	p := &peer{
		id:        peerID,
		processor: r,
		status:    status,
		picker:    picker,
		writer:    startStreamWriter(peerID, status, r, streamTypeMessage),
		pipeline:  pipeline,
		recvc:     make(chan Message, recvBufSize),
		propc:     make(chan Message, maxPendingProposals),
		resultc:   make(chan Message, recvBufSize),
		stopc:     make(chan struct{}),
	}

	p.reader = newStreamReader(peerID, transport, picker, status, p.recvc, p.propc)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() {
		for {
			select {
			case mm := <-p.recvc:
				var err error
				if err = r.Process(ctx, mm); err != nil {
					logger.Errorf("failed to process raft message (%v)", err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil {
					logger.Errorf("failed to process raft message (%v)", err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.reader.start()

	return p
}
func (p *peer) getStatus() *peerStatus {
	return p.status
}
func (p *peer) send(m Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	writec, name := p.pick(m)
	select {
	case writec <- m:
	default:
		p.processor.ReportUnreachable(m.To)

		if p.status.isActive() {
			logger.Errorf("dropped internal raft message to %s since %s's sending buffer is full (bad/overloaded network)", p.id, name)
		}
	}
}

func (p *peer) update(urls URLs) {
	p.picker.update(urls)
}

func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		logger.Panicf("unhandled stream type %s", conn.t)
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.reader.resume()
}

func (p *peer) stop() {

	defer logger.Infof("stopped peer %s", p.id)

	close(p.stopc)
	p.cancel()

	p.writer.stop()
	p.pipeline.stop()
	p.reader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
func (p *peer) pick(m Message) (writec chan<- Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.

	if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}

	return p.pipeline.msgc, pipelineMsg
}
