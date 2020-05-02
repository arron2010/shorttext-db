package network

type remote struct {
	id       ID
	status   *peerStatus
	pipeline *pipeline
}

func startRemote(tr *Transport, urls URLs, id ID) *remote {
	picker := newURLPicker(urls)
	status := newPeerStatus(id)
	pipeline := &pipeline{
		peerID:    id,
		tr:        tr,
		picker:    picker,
		status:    status,
		processor: tr.Processor,
		errorc:    tr.ErrorC,
	}
	pipeline.start()

	return &remote{
		id:       id,
		status:   status,
		pipeline: pipeline,
	}
}

func (g *remote) send(m Message) {
	//logger.Info("send message--->")
	select {
	case g.pipeline.msgc <- m:
	default:
		if g.status.isActive() {
			logger.Warningf("dropped internal raft message to %s since sending buffer is full (bad/overloaded network)", g.id)
		}
		logger.Debugf("dropped %s to %s since sending buffer is full", m.Type, g.id)
		//sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

func (g *remote) stop() {
	g.pipeline.stop()
}

func (g *remote) Pause() {
	g.stop()
}

func (g *remote) Resume() {
	g.pipeline.start()
}
