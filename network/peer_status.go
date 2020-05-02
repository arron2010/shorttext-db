package network

import (
	"fmt"
	"sync"
	"time"
)

type failureType struct {
	source string
	action string
}

type peerStatus struct {
	id     ID
	mu     sync.Mutex // protect variables below
	active bool
	since  time.Time
}

func newPeerStatus(id ID) *peerStatus {
	return &peerStatus{
		id: id,
	}
}

func (s *peerStatus) activate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {

		s.active = true
		s.since = time.Now()
	}
}

func (s *peerStatus) deactivate(failure failureType, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := fmt.Sprintf("deactivate: failed to %s %s on %s (%s)", failure.action, s.id, failure.source, reason)
	if s.active {
		logger.Errorf(msg)
		s.active = false
		s.since = time.Time{}
		return
	}

}

func (s *peerStatus) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *peerStatus) activeSince() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.since
}
