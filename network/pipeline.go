package network

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

type pipeline struct {
	peerID ID

	tr        *Transport
	picker    *urlPicker
	status    *peerStatus
	processor Processor
	errorc    chan error
	// deprecate when we depercate v2 API
	//followerStats *stats.FollowerStats

	msgc chan Message
	// wait for the handling routines
	wg    sync.WaitGroup
	stopc chan struct{}
}

func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	for i := 0; i < connPerPipeline; i++ {
		go p.handle()
	}
	//	logger.Infof("started HTTP pipelining with peer [%s]", p.peerID)
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()
	logger.Infof("stopped HTTP pipelining with peer [%s]", p.peerID)
}

func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc:

			err := p.post(MustMarshal(&m))
			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())
				p.processor.ReportUnreachable(m.To)
				continue
			}

			p.status.activate()

		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick()
	req := createPostRequest(u, ServerPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() {
		select {
		case <-done:
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()

	resp, err := p.tr.pipelineRt.RoundTrip(req)
	done <- struct{}{}
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	resp.Body.Close()

	err = checkPostResponse(resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
