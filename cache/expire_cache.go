package cache

import (
	"github.com/xp/shorttext-db/glogger"
	"sync"
	"time"
)

var logger = glogger.MustGetLogger("cache")

type expireCacheItem struct {
	item interface{}
	//stands for Time-To-Live
	ttl int64
}

func (e expireCacheItem) expire() bool {
	return time.Now().UnixNano() > e.ttl
}

type ExpireCache struct {
	ExpireDuration time.Duration
	items          map[uint64]expireCacheItem
	mu             sync.RWMutex
}

func NewExpireCache(expireDuration time.Duration) *ExpireCache {
	instance := &ExpireCache{}
	instance.items = make(map[uint64]expireCacheItem)
	instance.ExpireDuration = expireDuration
	go instance.clear()
	return instance
}
func (e *ExpireCache) Set(key uint64, x interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var cacheItem expireCacheItem
	cacheItem = expireCacheItem{item: x, ttl: time.Now().Add(e.ExpireDuration).UnixNano()}
	e.items[key] = cacheItem
}

func (e *ExpireCache) Get(key uint64) interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cacheItem, ok := e.items[key]
	if !ok {
		return nil
	}
	if cacheItem.expire() {
		return nil
	}
	return cacheItem.item
}

func (e *ExpireCache) clear() {
	tickc := time.NewTicker(time.Second * 30)
	defer tickc.Stop()
	for range tickc.C {
		e.mu.Lock()
		for k, v := range e.items {
			if v.expire() {

				delete(e.items, k)
				logger.Infof("清除缓存 key:%d\n", k)
			}
		}
		e.mu.Unlock()
	}
}
