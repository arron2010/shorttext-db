package network

import (
	"net/url"
	"sync"
)

type urlPicker struct {
	mu                 sync.Mutex // guards urls and picked
	urls               URLs
	picked             int
	supportUnreachable bool
}

func newURLPicker(urls URLs) *urlPicker {
	return &urlPicker{
		urls:               urls,
		supportUnreachable: false,
	}
}

func (p *urlPicker) update(urls URLs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = urls
	p.picked = 0
}

func (p *urlPicker) pick() url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.urls[p.picked]
}

//如果当前URL不可用，就使用其他可用的URL
func (p *urlPicker) unreachable(u url.URL) {
	//不考虑用其他节点进行替换　2020-03-31
	if p.supportUnreachable {
		p.mu.Lock()
		defer p.mu.Unlock()
		if u == p.urls[p.picked] {
			// 采用模运算，可以解决索引值不越界以及区别当前URL
			p.picked = (p.picked + 1) % len(p.urls)
		}
	}
}
