package network

import (
	"time"

	"log"
	"net/http"
	"net/url"
)

type StreamServer struct {
	transport *Transport

	Id        int
	Peers     []string
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

func NewStreamServer(id int, processor Processor, peers ...string) (*StreamServer, error) {
	instance := &StreamServer{}
	instance.Id = id
	instance.Peers = make([]string, 0, 0)

	instance.httpstopc = make(chan struct{})
	instance.httpdonec = make(chan struct{})
	for i := 0; i < len(peers); i++ {
		instance.Peers = append(instance.Peers, peers[i])

	}
	t := &Transport{
		ID:        ID(id),
		Processor: processor,
		ErrorC:    make(chan error),
	}
	t.DialRetryFrequency = 0.2
	t.DialTimeout = 15 * time.Second

	var err error
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return nil, err
	}
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout)
	if err != nil {
		return nil, err
	}
	t.remotes = make(map[ID]*remote)
	t.peers = make(map[ID]Peer)
	instance.transport = t

	return instance, err
}

func (this *StreamServer) GetTransport() *Transport {
	return this.transport
}

func (this *StreamServer) Start() {
	this.startPeers()
	go this.startListener()

}
func (this *StreamServer) ActivePeers() (cnt int) {
	return this.transport.ActivePeers()
}

func (this *StreamServer) DeactivePeers() []int {
	return this.transport.DeactivePeers()
}

func (this *StreamServer) IsPeerActive(peerId int) bool {
	return this.transport.IsPeerActive(peerId)
}
func (this *StreamServer) Send(msg Message) {
	this.transport.Send([]Message{msg})
}

func (this *StreamServer) startPeers() {
	peers := this.Peers
	for i := range peers {
		if i+1 != this.Id {
			this.transport.AddPeer(ID(i+1), []string{peers[i]})
		}
	}
}

func (this *StreamServer) StopHTTP() {
	this.transport.Stop()
	close(this.httpstopc)
	<-this.httpdonec
}

func (this *StreamServer) startListener() {
	url, err := url.Parse(this.Peers[this.Id-1])
	if err != nil {
		log.Fatalf("HttpTransport解析URL失败 (%v)", err)
	}
	ln, err := newStoppableListener(url.Host, this.httpstopc)
	if err != nil {
		log.Fatalf("HttpTransport侦听失败：(%v)", err)
	}
	err = (&http.Server{Handler: this.transport.Handler()}).Serve(ln)
	select {
	case <-this.httpstopc:
	default:
		log.Fatalf("HttpTransport服务不可用 ：(%v)", err)
	}
	close(this.httpdonec)
}
