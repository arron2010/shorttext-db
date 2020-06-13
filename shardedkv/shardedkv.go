package shardedkv

import (
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/glogger"
	"sync"
)

var logger = glogger.MustGetLogger("shardedkv")

type Object interface{}

//存储接口
type Storage interface {
	Open() error

	Get(key uint64, index uint64, item interface{}) (interface{}, error)

	Set(key uint64, index uint64, value interface{}) (error, uint64)

	Delete(key uint64, index uint64) error

	//ResetConnection(key uint64) error

	Close() error
}

type KVStore struct {
	continuum Chooser
	storages  map[string]Storage
	seq       filedb.SequenceSvc
	mu        sync.RWMutex
}

//分片选择器
type Chooser interface {
	// 设置分片的桶
	SetBuckets([]string) error
	//根据数据键，获取对应分片的桶
	Choose(key uint64) (string, uint64)
	// 获取分片的桶
	Buckets() []string
}

/*
一个节点最多存储rowCount条记录，rowCount均匀地分布在maxRange个数据库上
*/
type RangeChooser struct {
	maxRange   uint32
	rowCount   uint32
	shards     map[uint64]string
	buckets    []string
	start      uint32
	partitions []partition
}
type partition struct {
	begin uint64
	end   uint64
	name  string
}

const maxRangeValue = 3

func NewRangeChooser(maxRange uint32, rowCount uint32, start uint32) *RangeChooser {
	r := &RangeChooser{}
	if maxRange == 0 {
		maxRange = maxRangeValue
	}
	r.maxRange = maxRange
	r.rowCount = rowCount
	r.shards = make(map[uint64]string)
	r.start = start
	return r
}

func (r *RangeChooser) SetBuckets(names []string) error {
	var start uint64 = uint64(r.start)
	var partitions []partition = make([]partition, 0, 0)
	for i := 1; i <= len(names); i++ {
		partitionItem := partition{begin: start, end: start + uint64(r.rowCount) - 1, name: names[i-1]}
		start = uint64(i)*uint64(r.rowCount) + start
		partitions = append(partitions, partitionItem)
	}
	r.buckets = names
	r.partitions = partitions
	return nil
}

func (r *RangeChooser) Choose(key uint64) (string, uint64) {
	var shardName string
	var dbIndex uint64

	for i := 0; i < len(r.partitions); i++ {
		if key >= r.partitions[i].begin && key <= r.partitions[i].end {
			shardName = r.partitions[i].name
			break
		}
	}

	if len(shardName) == 0 {
		shardName = r.partitions[len(r.partitions)-1].name
	}
	dbIndex = key%uint64(r.maxRange) + 1
	logger.Infof("分区选择[Key:%d,ShardName:%s,DbIndex:%d]\n", key, shardName, dbIndex)
	return shardName, dbIndex
}

func (r *RangeChooser) Buckets() []string {
	return r.buckets
}

// 命名的分片存储
type Shard struct {
	Name    string
	Backend Storage
}

func New(chooser Chooser, seq filedb.SequenceSvc, shards []Shard) *KVStore {
	var buckets []string
	kv := &KVStore{
		continuum: chooser,
		storages:  make(map[string]Storage),
	}
	for _, shard := range shards {
		buckets = append(buckets, shard.Name)
		kv.AddShard(shard.Name, shard.Backend)
	}
	chooser.SetBuckets(buckets)
	kv.seq = seq
	return kv
}

func (kv *KVStore) Get(key uint64, item interface{}) (interface{}, error) {

	var storage Storage

	kv.mu.Lock()
	shard, index := kv.continuum.Choose(key)
	storage = kv.storages[shard]
	kv.mu.Unlock()

	return storage.Get(key, index, item)
}

func (kv *KVStore) Set(key uint64, val interface{}) (error, uint64) {

	var storage Storage
	kv.mu.Lock()
	if key == 0 {
		key = kv.seq.Next()
	}
	shard, index := kv.continuum.Choose(key)
	storage = kv.storages[shard]

	kv.mu.Unlock()

	return storage.Set(key, index, val)
}

func (kv *KVStore) Delete(key uint64) error {

	var storage Storage
	kv.mu.Lock()
	shard, index := kv.continuum.Choose(key)
	storage = kv.storages[shard]
	kv.mu.Unlock()

	err := storage.Delete(key, index)
	return err
}

//重新连接
//func (kv *KVStore) ResetConnection(key uint64) error {
//
//	var storage Storage
//
//	kv.mu.Lock()
//	shard := kv.continuum.Choose(key)
//	storage = kv.storages[shard]
//	kv.mu.Unlock()
//
//
//	return storage.ResetConnection(key)
//}

//增加分片
func (kv *KVStore) AddShard(shard string, storage Storage) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.storages[shard] = storage
}

//删除分片
func (kv *KVStore) DeleteShard(shard string) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.storages, shard)
}

func (kv *KVStore) Open() error {
	return nil
}

func (kv *KVStore) Close() error {
	return nil
}
