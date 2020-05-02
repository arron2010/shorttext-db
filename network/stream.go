package network

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/version"

	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type streamType string

func (t streamType) endpoint() string {
	switch t {
	case streamTypeMessage:
		return path.Join(StreamPrefix, "message")
	default:
		logger.Panicf("unhandled stream type %v", t)
		return ""
	}
}

func (t streamType) String() string {
	switch t {
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

func isLinkHeartbeatMessage(m *Message) bool {
	return m.Type == MsgHeartbeat && m.From == 0 && m.To == 0
}

type outgoingConn struct {
	t streamType
	io.Writer
	http.Flusher
	io.Closer
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	peerID    ID
	status    *peerStatus
	processor Processor

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer
	working bool

	msgc  chan Message
	connc chan *outgoingConn
	stopc chan struct{}
	done  chan struct{}
	typ   streamType
}

func newStreamWriter(id ID, status *peerStatus, processor Processor, typ streamType) *streamWriter {
	w := &streamWriter{
		peerID:    id,
		status:    status,
		processor: processor,
		msgc:      make(chan Message, streamBufSize),
		connc:     make(chan *outgoingConn),
		stopc:     make(chan struct{}),
		done:      make(chan struct{}),
		typ:       typ,
	}
	return w
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
func startStreamWriter(id ID, status *peerStatus, processor Processor, typ streamType) *streamWriter {
	w := newStreamWriter(id, status, processor, typ)
	go w.run()
	return w
}

func (cw *streamWriter) run() {
	var (
		msgc       chan Message
		heartbeatc <-chan time.Time
		t          streamType
		enc        encoder
		flusher    http.Flusher
		batched    int
	)
	tickc := time.NewTicker(ConnReadTimeout / 2)
	defer tickc.Stop()

	//	logger.Infof("started streaming with peer [%s] (%s writer)\n", cw.peerID, cw.typ)

	for {
		select {
		case <-heartbeatc:
			err := enc.encode(&linkHeartbeatMessage)
			if err == nil {
				flusher.Flush()
				batched = 0
				continue
			}
			logger.Errorf("心跳失败:peer %s (%s writer) error:%s\n", cw.peerID, t, err.Error())

			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			cw.close()
			heartbeatc, msgc = nil, nil

		case m := <-msgc:
			err := enc.encode(&m)
			if err == nil {
				if len(msgc) == 0 || batched > streamBufSize/2 {
					flusher.Flush()
					batched = 0
				} else {
					batched++
				}
				continue
			}
			logger.Errorf("写入Peer[%s] TCP流失败:%s \n", cw.status.id.String(), err.Error())

			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error())
			cw.close()
			heartbeatc, msgc = nil, nil
			cw.processor.ReportUnreachable(m.To)

		case conn := <-cw.connc:
			cw.mu.Lock()

			response := conn.Writer.(http.ResponseWriter)
			logger.Infof("streamWriter建立连接 Current Id:%s  From:%s Working:%v outgoingConn:%p\n",
				response.Header().Get(HTTP_HEADER_TO), cw.peerID.String(), cw.working, conn)
			closed := cw.closeUnlocked()
			t = conn.t
			switch conn.t {
			case streamTypeMessage:
				enc = &messageEncoder{w: conn.Writer}
			default:
				logger.Panicf("unhandled stream type %s", conn.t)
			}
			flusher = conn.Flusher
			//unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true
			cw.mu.Unlock()

			if closed {
				logger.Errorf("closed an existing TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}

			heartbeatc, msgc = tickc.C, cw.msgc
		case <-cw.stopc:
			if cw.close() {
				logger.Errorf("closed the TCP streaming connection with peer %s (%s writer)", cw.peerID, t)
			}
			close(cw.done)
			return
		}
	}
}

func (cw *streamWriter) writec() (chan<- Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {

	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {

	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		logger.Errorf("peer %s (writer) connection close error: %v", cw.peerID, err)
	}
	if len(cw.msgc) > 0 {
		cw.processor.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan Message, streamBufSize)
	cw.working = false

	return true
}

func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	peerID ID
	typ    streamType

	tr     *Transport
	picker *urlPicker
	status *peerStatus
	recvc  chan<- Message
	propc  chan<- Message

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func newStreamReader(peerID ID, tr *Transport, picker *urlPicker, status *peerStatus, recvc chan<- Message, propc chan<- Message) *streamReader {
	msgReader := &streamReader{
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     tr,
		picker: picker,
		status: status,
		recvc:  recvc,
		propc:  propc,
		rl:     rate.NewLimiter(tr.DialRetryFrequency, 1),
	}
	return msgReader
}

func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ

	for {
		rc, err := cr.dial(t)

		if err != nil {
			url := path.Join(t.endpoint(), cr.peerID.String())
			if err == errUnsupportedStreamType {
				panic(fmt.Sprintf("节点版本存在不一致 连接URL:%s\n", url))
			} else {
				logger.Errorf("连接失败 URL:%s 错误:%s \n", url, err.Error())
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {
			cr.status.activate()
			err = cr.decodeLoop(rc, t)
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			case IsClosedConnError(err):
				logger.Errorf("TCP  connection closed with peer [%s] (%s reader)", cr.peerID, cr.typ)
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
				//rc.Close()
				logger.Errorf("读取TCP流异常:%s  被读取通道ID:%s\n", err.Error(), cr.status.id.String())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			logger.Errorf("stopped streaming with peer [%s] (%s reader)", cr.peerID, t)
			close(cr.done)

			return
		}
		if err != nil {
			logger.Errorf("streaming with peer [%s] (%s reader) rate limiter error: %v", cr.peerID, t, err)
		}
	}
}

func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	switch t {
	case streamTypeMessage:
		dec = &messageDecoder{r: rc}
	default:
		logger.Panicf("unhandled stream type %s", t)
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()
	var count int
	for {
		m, err := dec.decode()
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			logger.Error("streamReader反序列化失败", err)
			return err
		}

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused {
			continue
		}
		count++

		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		recvc := cr.recvc
		if m.Type == MsgProp {
			recvc = cr.propc
		}

		select {
		case recvc <- m:
		default:
			if cr.status.isActive() {
				logger.Errorf("dropped internal raft message from %s since receiving buffer is full (overloaded network)",
					ID(m.From))
			}
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick()
	uu := u
	uu.Path = path.Join(t.endpoint(), cr.tr.ID.String())

	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	req.Header.Set(HTTP_HEADER_FROM, cr.tr.ID.String())
	req.Header.Set(HTTP_HEADER_VERSION, ServerVersion)

	req.Header.Set(HTTP_HEADER_TO, cr.peerID.String())

	setPeerURLsHeader(req, cr.tr.URLs)

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		logger.Errorf("节点[%s]访问节点[%s %s]失败:%s\n", cr.peerID.String(), cr.tr.ID.String(), uu.String(), err.Error())
		return nil, err
	}

	rv := serverVersion(resp.Header)
	lv := version.Must(version.NewVersion(ServerVersion))
	if compareMajorMinorVersion(rv, lv) == -1 && !checkStreamSupport(rv, t) {
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}

	switch resp.StatusCode {
	case http.StatusGone:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotFound:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer [%s] failed to find local node %s", cr.peerID, cr.tr.ID)
	case http.StatusPreconditionFailed:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			logger.Errorf("request sent was ignored by peer [%s] (server version incompatible)", cr.peerID)
			return nil, errIncompatibleVersion
		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}
	default:
		GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			logger.Errorf("peer [%s] (reader) connection close error: %v", cr.peerID, err)
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}

// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *version.Version, t streamType) bool {
	nv := &version.Version{Major: v.Major, Minor: v.Minor}
	for _, s := range supportedStream[nv.String()] {
		if s == t {
			return true
		}
	}
	return false
}
