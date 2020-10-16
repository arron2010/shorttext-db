package task

import (
	proto2 "github.com/golang/protobuf/proto"
	"github.com/xp/shorttext-db/easymr/grpc/proto"
	"github.com/xp/shorttext-db/glogger"
	"reflect"
)

var logger = glogger.MustGetLogger("task")

type ISerializable interface {
	Serialize(source interface{}) ([]byte, error)
	Deserialize(typeName string, payload []byte) (interface{}, error)
}

const (
	SOURCE_SERIALIZE = 1
	RESULT_SERIALIZE = 2
	NONE_SERIALIZE   = 3
	FULL_SERIALIZE   = 4
)

type MessageEncoder struct {
	Serializer ISerializable
}

func (this *MessageEncoder) TaskObjToByte() ([]byte, []byte) {
	return nil, nil
}

func (this *MessageEncoder) ByteToTaskObj(stream []byte) *Task {
	return nil
}

func (this *MessageEncoder) Encode(source *map[int]*Task, mode int) (payload *TaskPayload, err error) {
	taskMap := &proto.TaskMap{}

	taskMap.Content = make(map[uint32]*proto.Task)
	//begin := time.Now()
	var binaryBlock []byte = []byte{}
	var taskItem *proto.Task

	for key, value := range *source {
		taskItem, binaryBlock, err = this.toProtoTask(value, mode)
		if err != nil {
			return nil, err
		}
		taskMap.Content[uint32(key)] = taskItem

		//index := value.Context.Context[constants.TASK_INDEX]
		//nIndex := index.(int)
		//if nIndex != key{
		//	logger.Infof("test Encode key1:%d key2:%d",key,nIndex)
		//}

	}
	// elapsed := time.Since(begin)
	//logger.Infof("toProtoTask对象序列化消耗时间:%.2f\n",elapsed.Seconds())
	// begin = time.Now()
	buffer, err := proto2.Marshal(taskMap)
	if err != nil {
		logger.Errorf("message encode error,%s\n", err)
		return nil, err
	}

	payload = &TaskPayload{
		Payload:    buffer,
		BigPayload: binaryBlock,
	}

	//elapsed = time.Since(begin)
	// logger.Infof("taskMap对象序列化消耗时间:%.2f\n",elapsed.Seconds())
	return payload, nil
}

func (this *MessageEncoder) Decode(buf []byte, mode int) (*map[int]*Task, error) {
	taskMap := proto.TaskMap{}
	err := proto2.Unmarshal(buf, &taskMap)
	source := make(map[int]*Task)
	if err != nil {
		logger.Errorf("message encode error,%s\n", err)
		return &source, err
	}
	for key, value := range taskMap.Content {
		taskItem, err := this.toTask(value, mode)
		if err != nil {
			return nil, err
		}
		//index := taskItem.Context.Context[constants.TASK_INDEX]
		//logger.Infof("test Decode key1:%d key2:%d",key,index.(int))

		//taskItem.Context.Context[constants.TASK_INDEX] = taskItem.Index
		//taskItem.Context.Context[constants.TASK_TERM] = taskItem.Term

		source[int(key)] = taskItem
	}
	return &source, nil
}

func (this *MessageEncoder) collectionToByteStream(collection *Collection) ([]*proto.ObjectItem, error) {

	var bufferItem []*proto.ObjectItem
	bufferItem = make([]*proto.ObjectItem, collection.Length())
	bufferItem = bufferItem[:0]
	for _, sourceItem := range *collection {
		buffer, err := this.Serializer.Serialize(sourceItem)
		if err != nil {
			return nil, err
		}
		item := &proto.ObjectItem{}
		itemType := reflect.TypeOf(sourceItem)
		item.Type = itemType.String()
		item.Content = buffer
		bufferItem = append(bufferItem, item)
	}
	return bufferItem, nil
}

func (this *MessageEncoder) byteStreamToCollection(bufferItem []*proto.ObjectItem) (*Collection, error) {

	var collection *Collection = NewCollection()
	if len(bufferItem) == 0 {
		return collection, nil
	}
	for _, buffer := range bufferItem {
		item, err := this.Serializer.Deserialize(buffer.Type, buffer.Content)
		if err != nil {
			return nil, err
		}
		collection.Append(item)
	}
	return collection, nil
}

func (this *MessageEncoder) toProtoTask(taskItem *Task, mode int) (*proto.Task, []byte, error) {
	var source *proto.Collection = &proto.Collection{}
	var result *proto.Collection = &proto.Collection{}
	var context *proto.TaskContext = &proto.TaskContext{}
	var err error
	switch mode {
	case SOURCE_SERIALIZE:
		source.Content, err = this.collectionToByteStream(&taskItem.Source)
		if err != nil {
			return nil, nil, err
		}
	case RESULT_SERIALIZE:
		result.Content, err = this.collectionToByteStream(&taskItem.Result)
		if err != nil {
			return nil, nil, err
		}
	case FULL_SERIALIZE:
		source.Content, err = this.collectionToByteStream(&taskItem.Source)
		if err != nil {
			return nil, nil, err
		}
		result.Content, err = this.collectionToByteStream(&taskItem.Result)
		if err != nil {
			return nil, nil, err
		}
	default:
		source.Content = make([]*proto.ObjectItem, 0)
		result.Content = make([]*proto.ObjectItem, 0)
	}

	context.Context = make(map[string]*proto.ObjectItem)

	for key, value := range taskItem.Context.Context {
		buffer, err := this.Serializer.Serialize(value)
		if err != nil {
			return nil, nil, err
		}
		contextValue := &proto.ObjectItem{}
		contextValue.Type = reflect.TypeOf(value).Name()
		contextValue.Content = buffer
		context.Context[key] = contextValue
	}

	protoTask := &proto.Task{}
	protoTask.Type = uint32(taskItem.Type)
	protoTask.Priority = uint32(taskItem.Priority)
	protoTask.Consumable = taskItem.Consumable
	protoTask.Source = source
	protoTask.Result = result
	protoTask.Context = context
	protoTask.Stage = uint32(taskItem.Stage)
	protoTask.RunType = uint32(taskItem.RunType)
	protoTask.TimeOut = uint32(taskItem.TimeOut)
	protoTask.Term = uint64(taskItem.Context.Term)
	protoTask.Index = uint64(taskItem.Context.Index)

	return protoTask, nil, nil
}

func (this *MessageEncoder) toTask(protoItem *proto.Task, mode int) (*Task, error) {
	var taskItem *Task = &Task{}

	var source *Collection = NewCollection()
	var result *Collection = NewCollection()
	var context *TaskContext = &TaskContext{}
	var err error
	switch mode {
	case SOURCE_SERIALIZE:
		source, err = this.byteStreamToCollection(protoItem.Source.Content)
		if err != nil {
			return nil, err
		}
	case RESULT_SERIALIZE:
		result, err = this.byteStreamToCollection(protoItem.Result.Content)
		if err != nil {
			return nil, err
		}
	case FULL_SERIALIZE:
		source, err = this.byteStreamToCollection(protoItem.Source.Content)
		if err != nil {
			return nil, err
		}
		result, err = this.byteStreamToCollection(protoItem.Result.Content)
		if err != nil {
			return nil, err
		}
	default:
		source = NewCollection()
		result = NewCollection()
	}

	context.Context = make(map[string]interface{})
	for key, item := range protoItem.Context.Context {
		context.Context[key], err = this.Serializer.Deserialize(item.Type, item.Content)
		if err != nil {
			return nil, err
		}
	}

	taskItem.Type = taskType(protoItem.Type)
	taskItem.Stage = int(protoItem.Stage)
	taskItem.Priority = taskPriority(protoItem.Priority)
	taskItem.Consumable = protoItem.Consumable
	taskItem.Source = *source
	taskItem.Result = *result
	taskItem.Context = context
	taskItem.RunType = int(protoItem.RunType)
	taskItem.TimeOut = int(protoItem.TimeOut)
	taskItem.Context.Term = int(protoItem.Term)
	taskItem.Context.Index = int(protoItem.Index)
	return taskItem, nil
}

//type BlockCache struct {
//	redisdb    *redis.Client
//	pipeline   redis.Pipeliner
//	LocalCache map[string][]byte
//	L2Cache    bool
//}

//var once sync.Once
//var blockCache *BlockCache

//func NewBlockCache(addr string, password string) *BlockCache {
//	once.Do(func() {
//		redisdb := redis.NewClient(&redis.Options{
//			Addr:               addr,
//			Password:           password,
//			DialTimeout:        60 * time.Second,
//			ReadTimeout:        60 * time.Second,
//			WriteTimeout:       60 * time.Second,
//			PoolSize:           1500,
//			MaxRetries:         5,
//			MinIdleConns:       30,
//			IdleCheckFrequency: 10 * time.Second,
//			IdleTimeout:        60 * time.Second,
//			PoolTimeout:        65 * time.Second,
//		})
//		blockCache = &BlockCache{}
//		blockCache.redisdb = redisdb
//		blockCache.pipeline = redisdb.Pipeline()
//		blockCache.LocalCache = make(map[string][]byte)
//		blockCache.L2Cache = true
//	})
//
//	return blockCache
//}
//func (block *BlockCache) PutLocal(prefix uint32, index int, data []byte) {
//	var strKey string = block.createKey(prefix, index)
//	block.LocalCache[strKey] = data
//
//}
//
//func (block *BlockCache) GetLocal(prefix uint32, index int) []byte {
//	var strKey string = block.createKey(prefix, index)
//	data, ok := block.LocalCache[strKey]
//	if !ok {
//		return []byte{}
//	} else {
//		return data
//	}
//}
//
//func (block *BlockCache) Put(prefix uint32, index int, data []byte, shared bool) error {
//	var err error
//	if shared {
//		var strKey string = block.createKey(prefix, index)
//		err = block.pipeline.Set(strKey, data, 0).Err()
//	}
//	if block.L2Cache && err == nil {
//		block.PutLocal(prefix, index, data)
//	}
//	return err
//}
//func (block *BlockCache) Execute() (int, error) {
//	cmds, err := block.pipeline.Exec()
//	return len(cmds), err
//}
//func (block *BlockCache) Clear() {
//	block.pipeline.FlushAll()
//}
//func (block *BlockCache) KeyCount(prefix uint32) int {
//	strKey := "task_" + strconv.FormatUint(uint64(prefix), 10) + "_*"
//	keys, err := block.redisdb.Keys(strKey).Result()
//	if err != nil {
//		logger.Error("获取任务缓存数据失败：", err)
//	}
//
//	return len(keys)
//}
//func (block *BlockCache) createKey(prefix uint32, index int) string {
//	strKey := "task_" + strconv.FormatUint(uint64(prefix), 10) + "_" + strconv.FormatUint(uint64(index), 10)
//	return strKey
//}
//func (block *BlockCache) Get(prefix uint32, index int) ([]byte, error) {
//	var strKey string = block.createKey(prefix, index)
//	var data []byte
//	var err error
//	if block.L2Cache {
//		data = block.GetLocal(prefix, index)
//		if len(data) == 0 {
//			data, err = block.redisdb.Get(strKey).Bytes()
//		}
//	} else {
//		data, err = block.redisdb.Get(strKey).Bytes()
//	}
//	return data, err
//}
//
//func (block *BlockCache) Close() {
//	block.redisdb.Close()
//}
