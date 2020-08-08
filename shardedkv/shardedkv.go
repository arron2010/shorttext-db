package shardedkv

import (
	"github.com/xp/shorttext-db/api"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/glogger"
	"strconv"
	"sync"
)

var logger = glogger.MustGetLogger("shardedkv")

type Object interface{}

type KVStore struct {
	continuum api.Chooser
	storages  map[string]api.Storage
	seq       filedb.SequenceSvc
	mu        sync.RWMutex
	name      string
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
	//	logger.Infof("分区选择[Key:%d,ShardName:%s,DbIndex:%d]\n", key, shardName, dbIndex)
	return shardName, dbIndex
}

func (r *RangeChooser) Buckets() []string {
	return r.buckets
}

// 命名的分片存储
type Shard struct {
	Name    string
	Backend api.Storage
}

func New(name string, chooser api.Chooser, seq filedb.SequenceSvc, shards []Shard) api.IKVStoreClient {
	var buckets []string
	kv := &KVStore{
		continuum: chooser,
		storages:  make(map[string]api.Storage),
	}
	for _, shard := range shards {
		buckets = append(buckets, shard.Name)
		kv.AddShard(shard.Name, shard.Backend)
	}
	chooser.SetBuckets(buckets)
	kv.seq = seq
	kv.name = name
	return kv
}

func (kv *KVStore) Get(nKey uint64, item interface{}) (interface{}, error) {

	var storage api.Storage
	kv.mu.Lock()
	shard, index := kv.continuum.Choose(nKey)
	storage = kv.storages[shard]
	kv.mu.Unlock()
	key := strconv.FormatUint(nKey, 10)
	return storage.Get(key, index, item)
}

func (kv *KVStore) Next() uint64 {
	return kv.seq.Next(kv.name)
}

func (kv *KVStore) IniSeq(val uint64) {
	kv.seq.SetStart(kv.name, val)
}

func (kv *KVStore) Set(nKey uint64, val interface{}) (error, uint64) {
	var storage api.Storage

	kv.mu.Lock()
	if nKey == 0 {
		nKey = kv.seq.Next(kv.name)
	}
	key := strconv.FormatUint(nKey, 10)
	shard, index := kv.continuum.Choose(nKey)
	storage = kv.storages[shard]
	kv.mu.Unlock()
	err, _ := storage.Set(key, index, val)
	return err, nKey
}
func (kv *KVStore) SetText(nKey uint64, val string) error {
	var storage api.Storage

	kv.mu.Lock()
	key := strconv.FormatUint(nKey, 10)
	shard, index := kv.continuum.Choose(nKey)
	storage = kv.storages[shard]
	kv.mu.Unlock()
	return storage.SetText(key, val, index)
}

func (kv *KVStore) GetText(nKey uint64) string {
	var storage api.Storage
	kv.mu.Lock()
	shard, index := kv.continuum.Choose(nKey)
	storage = kv.storages[shard]
	kv.mu.Unlock()
	key := strconv.FormatUint(nKey, 10)
	return storage.GetText(key, index)
}

func (kv *KVStore) Delete(key string) error {
	var storage api.Storage
	kv.mu.Lock()
	nKey, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		return err
	}
	shard, index := kv.continuum.Choose(nKey)
	storage = kv.storages[shard]
	kv.mu.Unlock()
	err = storage.Delete(key, index)
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
func (kv *KVStore) AddShard(shard string, storage api.Storage) {

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
