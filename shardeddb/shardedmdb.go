package shardeddb

import (
	"context"
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/api"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/shardedkv"
	"github.com/xp/shorttext-db/utils"
	"strconv"
	"time"
)

var logger = glogger.MustGetLogger("shardeddb")

//流通道处理者
type dbNodeHandler struct {
	channel   *network.StreamServer
	dbs       map[string]IMemStorage
	clbt      *collaborator.Collaborator
	defaultDB string
	dbCount   int
}

func newDBNodeHandler(id int, dbCount int, path string, names ...string) *dbNodeHandler {
	d := &dbNodeHandler{}
	d.dbs = make(map[string]IMemStorage)

	d.dbCount = dbCount

	var i int
	for _, name := range names {
		for i = 1; i <= d.dbCount; i++ {
			dbName := name + "_" + strconv.FormatUint(uint64(i), 10)
			dbInstance, err := newMemStorage(id, path, dbName)
			if err != nil {
				logger.Errorf("创建数据库实例[%s]失败:%s\n", dbName, err.Error())
				continue
			}
			d.dbs[dbName] = dbInstance
		}
	}

	return d
}

func (d *dbNodeHandler) getCount(db string) []int {
	countList := make([]int, 0, len(d.dbs))
	for i := 1; i <= d.dbCount; i++ {
		dbName := db + "_" + strconv.FormatUint(uint64(i), 10)
		store := d.dbs[dbName]
		countList = append(countList, store.GetKeyCount())
	}
	return countList

}
func (d *dbNodeHandler) find(db string, text string) ([]entities.Record, error) {
	if len(db) == 0 {
		db = d.defaultDB
	}
	jobInfo := &interfaces.JobInfo{}
	jobInfo.Handler = "LookupJob"
	jobInfo.Params = make(map[string]string)
	jobInfo.Context = make(map[string][]byte)
	jobInfo.LocalJob = true
	p := &findParam{}
	p.Text = text
	p.DBName = db
	jobInfo.Source = p

	context := &task.TaskContext{}
	context.Context = make(map[string]interface{})
	result, err := d.clbt.MapReduce(jobInfo, context)
	return result.Content.([]entities.Record), err
}

func (d *dbNodeHandler) Process(ctx context.Context, m network.Message) error {
	db, ok := d.dbs[m.DBName]
	var err error
	var val string
	var errMsg string
	result := network.Message{}
	result.To = m.From
	result.From = m.To
	result.Count = m.Count
	result.Term = m.Term
	result.ResultCode = config.MSG_KV_RESULT_SUCCESS
	result.Index = m.Index
	result.Key = m.Key
	if !ok {
		result.ResultCode = config.MSG_KV_RESULT_FAILURE
		errMsg = fmt.Sprintf("数据库实例[%s]不存在", m.DBName)
		result.Text = errMsg
		d.channel.Send(result)
		return errors.New(errMsg)
	}
	key := m.Key

	switch m.Type {
	case config.MSG_KV_SET:
		err = db.Set(key, m.Text)
		logger.Infof("数据库[%s]更新数据:[key:%s,text:%s]\n", m.DBName, m.Key, m.Text)
	case config.MSG_KV_TEXTSET:
		err = db.SetWithIndex(key, m.Text, config.GJSON_FIELD_DESC)
		logger.Infof("数据库[%s] 带前缀更新数据:[key:%s,text:%s]\n", m.DBName, m.Key, m.Text)

	case config.MSG_KV_GET, config.MSG_KV_TEXTGET:
		val, err = db.Get(key)
		logger.Infof("数据库[%s]获取数据:[key:%s,text:%s]\n", m.DBName, m.Key, val)
	case config.MSG_KV_DEL:
		logger.Infof("数据库[%s]删除数据:[key:%s]\n", m.DBName, m.Key)
		err = db.Delete(key)
	case config.MSG_KV_ClOSE:
		err = db.Close()
	default:
		err = errors.New(fmt.Sprintf("数据库[%s]不支持该操作[%d]", m.DBName, m.Type))
	}
	if err != nil {
		result.Type = config.MSG_KV_RESULT_FAILURE
		result.Text = err.Error()
	} else {
		result.Text = val
	}
	d.channel.Send(result)

	return err
}
func (d *dbNodeHandler) ReportUnreachable(id uint64) {

}

type IRegionDBNode interface {
	FindText(prefix []string) []string
}

type dbNodeClient struct {
	client *proxy.StreamClient
	Id     uint64
	dbName string
}

func newDBNodeClient(id uint64, dbName string, client *proxy.StreamClient) *dbNodeClient {
	d := &dbNodeClient{}
	d.client = client
	d.Id = id
	d.dbName = dbName
	return d
}

func (d *dbNodeClient) Open() error {
	return nil
}
func (d *dbNodeClient) get(key string, index uint64, msgType uint32, item interface{}) (interface{}, error) {
	var err error
	var result *network.BatchMessage
	term, err := d.generateId()
	if err != nil {
		return nil, err
	}
	text := ""
	m := network.NewOnlyOneMsg(term, key, text, msgType)
	m.Messages[0].From = config.GetCase().GetMaster().ID
	m.Messages[0].To = d.Id
	m.Messages[0].DBName = d.dbName + "_" + strconv.FormatUint(index, 10)
	result, err = d.client.Send(m)
	if err != nil {
		return nil, err
	}
	if result == nil || len(result.Messages) == 0 {
		return nil, errors.New(fmt.Sprintf("dbNodeClient Get操作失败[Key:%s]", key))
	}
	//buff := util.StringToBytes(result.Messages[0].Text)
	//err = json.Unmarshal(buff, item)
	item, err = deserialize(result.Messages[0].Text, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *dbNodeClient) set(key string, index uint64, msgType uint32, value interface{}) (error, string) {
	var result *network.BatchMessage
	text, err := serialize(value)
	if err != nil {
		return err, ""
	}

	term, err := d.generateId()
	if err != nil {
		return err, ""
	}
	m := network.NewOnlyOneMsg(term, key, text, msgType)
	m.Messages[0].From = config.GetCase().GetMaster().ID
	m.Messages[0].To = d.Id
	m.Messages[0].DBName = d.dbName + "_" + strconv.FormatUint(index, 10)

	result, err = d.client.Send(m)
	if err != nil {
		return err, ""
	}
	if result == nil || len(result.Messages) == 0 {
		return errors.New(fmt.Sprintf("dbNodeClient Set操作失败[Key:%s]", key)), ""
	}
	resultMsg := result.Messages[0]

	if resultMsg.ResultCode == config.MSG_KV_RESULT_FAILURE {
		return errors.New(resultMsg.Text), ""
	}
	return nil, resultMsg.Key
}
func (d *dbNodeClient) GetText(key string, index uint64) string {
	text, err := d.get(key, index, config.MSG_KV_TEXTGET, nil)
	if err != nil {
		return ""
	}
	return text.(string)
}

func (d *dbNodeClient) SetText(key string, value string, index uint64) error {
	err, _ := d.set(key, index, config.MSG_KV_TEXTSET, value)
	return err
}

func (d *dbNodeClient) Get(key string, index uint64, item interface{}) (interface{}, error) {
	return d.get(key, index, config.MSG_KV_GET, item)
}
func (d *dbNodeClient) Set(key string, index uint64, value interface{}) (error, string) {
	return d.set(key, index, config.MSG_KV_SET, value)
}

func (d *dbNodeClient) Delete(key string, index uint64) error {
	var err error
	var result *network.BatchMessage

	term, err := d.generateId()
	if err != nil {
		return err
	}

	m := network.NewOnlyOneMsg(term, key, "", config.MSG_KV_DEL)
	m.Messages[0].From = config.GetCase().GetMaster().ID
	m.Messages[0].To = d.Id
	m.Messages[0].DBName = d.dbName + "_" + strconv.FormatUint(index, 10)
	result, err = d.client.Send(m)
	if err != nil {
		return err
	}
	if result == nil || len(result.Messages) == 0 {
		return errors.New(fmt.Sprintf("dbNodeClient Delete操作失败[Key:%s]", key))
	}
	resultMsg := result.Messages[0]
	if resultMsg.ResultCode == config.MSG_KV_RESULT_FAILURE {
		return errors.New(resultMsg.Text)
	}
	return nil
}

func (d *dbNodeClient) Close() error {
	return nil
}

func (d *dbNodeClient) generateId() (uint64, error) {

	node, err := utils.NewNode(0)
	if err != nil {
		return 0, err
	}
	id := uint64(node.Generate().Int64())
	return id, nil
}

//func (d *dbNodeClient) ResetConnection(key uint64) error {
//	return nil
//}

func newShard(id uint64, name string, dbName string, client *proxy.StreamClient) shardedkv.Shard {
	shard := shardedkv.Shard{}
	shard.Name = name
	backend := newDBNodeClient(id, dbName, client)
	shard.Backend = backend
	return shard
}

func newShards(dbName string) ([]shardedkv.Shard, error) {
	shards := make([]shardedkv.Shard, 0, 8)
	c := config.GetCase()
	cf := config.GetConfig()
	client, err := proxy.NewStreamClient(cf.KVServerAddr,
		time.Duration(cf.KVTimeout)*time.Second,
		time.Duration(cf.KVIdleTimeout)*time.Second)
	if err != nil {
		return nil, err
	}
	cards := c.GetCardList()
	for _, card := range cards {
		shard := newShard(card.ID, card.Name, dbName, client)
		shards = append(shards, shard)
	}
	return shards, err
}

//创建KV数据访问客户端
func newKVStore(dbName string, maxRange uint32, rowCount uint32, start uint32, sequenceServer string) (api.IKVStoreClient, error) {
	shards, err := newShards(dbName)
	if err != nil {
		return nil, err
	}
	chooser := shardedkv.NewRangeChooser(
		maxRange,
		rowCount,
		start)
	seq, err := filedb.NewSequenceProxy(sequenceServer)
	if err != nil {
		return nil, err
	}
	kv := shardedkv.New(dbName, chooser, seq, shards)
	return kv, err
}

func NewKVStore(dbName string) (api.IKVStoreClient, error) {
	return newKVStore(dbName,
		uint32(config.GetConfig().KVDBMaxRange),
		uint32(config.GetConfig().KVDBRowCount),
		uint32(config.GetConfig().KVDRowStart), config.GetConfig().SequenceServer)
}
