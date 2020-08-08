package api

//分片选择器
type Chooser interface {
	// 设置分片的桶
	SetBuckets([]string) error
	//根据数据键，获取对应分片的桶
	Choose(key uint64) (string, uint64)
	// 获取分片的桶
	Buckets() []string
}

//存储接口
type Storage interface {
	Open() error

	Get(key string, index uint64, item interface{}) (interface{}, error)

	Set(key string, index uint64, value interface{}) (error, string)

	Delete(key string, index uint64) error

	//ResetConnection(key uint64) error

	GetText(key string, index uint64) string

	SetText(key string, value string, index uint64) error

	Close() error
}

type IKVStoreClient interface {
	Get(nKey uint64, item interface{}) (interface{}, error)
	Set(nKey uint64, val interface{}) (error, uint64)

	SetText(nKey uint64, val string) error
	GetText(nKey uint64) string
}
