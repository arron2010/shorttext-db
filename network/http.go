package network

import (
	pioutil "com.neep/goplatform/goraft/ioutil"

	"context"
	"io/ioutil"
	"net/http"
	"path"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024
)

type peerGetter interface {
	Get(id ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

type pipelineHandler struct {
	tr        Transporter
	processor Processor
	cid       ID
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(tr Transporter, processor Processor, cid ID) http.Handler {
	return &pipelineHandler{
		tr:        tr,
		processor: processor,
		cid:       cid,
	}
}

func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	// Limit the data size that could be read from the request body, which ensures that read from
	// connection will not time out accidentally due to possible blocking in underlying implementation.
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		logger.Errorf("failed to read raft message (%v)", err)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		//recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	var m *Message = &Message{}
	if err := Unmarshal(m, b); err != nil {
		logger.Errorf("failed to unmarshal raft message (%v)", err)
		http.Error(w, "error unmarshaling raft message", http.StatusBadRequest)
		//recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	//receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	if err := h.processor.Process(context.TODO(), *m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			logger.Warningf("failed to process raft message (%v)", err)
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
			panic(err)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent)
}

type streamHandler struct {
	tr         *Transport
	peerGetter peerGetter
	processor  Processor
	id         ID
	cid        ID
}

func newStreamHandler(tr *Transport, pg peerGetter, processor Processor, id, cid ID) http.Handler {
	return &streamHandler{
		tr:         tr,
		peerGetter: pg,
		processor:  processor,
		id:         id,
		cid:        cid,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set(HTTP_HEADER_VERSION, ServerVersion)

	var t streamType
	switch path.Dir(r.URL.Path) {
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		logger.Debugf("ignored unexpected streaming request path %s", r.URL.Path)
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	fromStr := path.Base(r.URL.Path)
	from, err := IDFromString(fromStr)

	gto := r.Header.Get(HTTP_HEADER_TO)
	w.Header().Set(HTTP_HEADER_TO, gto)

	logger.Infof("接收请求:ID[%s] URL[%s] From[%d]\n", h.id.String(), r.URL.String(), from)
	if err != nil {
		logger.Errorf("failed to parse from %s into ID (%v)", fromStr, err)
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}

	p := h.peerGetter.Get(from)
	if p == nil {

		logger.Errorf("failed to find member %s in cluster %s", from, h.cid)
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	wto := h.id.String()
	if gto != wto {
		logger.Errorf("streaming request ignored (ID mismatch got %s want %s)", gto, wto)
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
	}
	p.attachOutgoingConn(conn)
	<-c.closeNotify()
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
