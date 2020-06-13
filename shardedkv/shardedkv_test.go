package shardedkv

import "testing"

func TestChooser(t *testing.T) {
	var maxRange uint32 = 3
	var rowCount uint32 = 15
	names := []string{"test1", "test2", "test3"}
	c := NewRangeChooser(maxRange, rowCount, 10)
	c.SetBuckets(names)
	for i := 1; i <= 30; i++ {
		shard, index := c.Choose(uint64(i))
		logger.Infof("shard:%s  index:%d\n", shard, index)
	}
}
