package network

import (
	"errors"
	"fmt"
	"path"
	"time"
)

const (
	ServerVersion                = "1.0.0"
	streamTypeMessage streamType = "message"
	streamBufSize                = 4096
)

const (
	MsgHeartbeat = 1000
	MsgProp      = 1001
)

const (
	HTTP_HEADER_FROM    = "X-Server-From"
	HTTP_HEADER_TO      = "X-Server-To"
	HTTP_HEADER_VERSION = "X-Server-Version"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	ConnReadTimeout    = 60 * time.Second
	ConnWriteTimeout   = 60 * time.Second
	PeerHeartbeatTime  = 3 * time.Second
	DialRetryFrequency = 0.2
	recvBufSize        = 4096

	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	mqTimeout = 30

	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
)

var (
	ServerPrefix  = "/xp"
	ProbingPrefix = path.Join(ServerPrefix, "probing")
	StreamPrefix  = path.Join(ServerPrefix, "stream")
)

var (
	errIncompatibleVersion   = errors.New("incompatible version")
	errMemberRemoved         = fmt.Errorf("the member has been permanently removed from the cluster")
	errMemberNotFound        = fmt.Errorf("member not found")
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")
	errMessageNotFound       = errors.New("message not found")
	errMessageCountNotEnough = errors.New("message count not enough")
)

var (
	supportedStream = map[string][]streamType{
		"2.0.0": {},
	}
)

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = Message{Type: MsgHeartbeat}
)
