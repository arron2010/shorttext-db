package cache

type MemoryMQ interface {
	Put(key uint64, value interface{})
	Get(key uint64) error
	Has(Key uint64) bool
}
