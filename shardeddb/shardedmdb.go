package shardeddb

import (
	"com.neep/goplatform/util"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/shardedkv"
	"strconv"

	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/memdb"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/utils"
	"os"
	"time"
)

var logger = glogger.MustGetLogger("shardeddb")

//对内存数据库的封装,提供简易接口
type memStorage struct {
	name string
	db   *memdb.DB
	path string
}

func newMemStorage(id int, path string, name string) (*memStorage, error) {
	m := &memStorage{}
	m.name = name
	m.path = path + "/" + strconv.Itoa(id) + "/" + m.name + ".db"
	m.name = name
	err := m.open()
	if err != nil {
		return nil, err
	}
	go m.autoPersistent()
	return m, nil
}

func (m *memStorage) autoPersistent() {
	heartbeat := time.NewTicker(time.Second * 3000)
	defer heartbeat.Stop()
	for range heartbeat.C {
		m.save()
	}
}
func (m *memStorage) open() error {
	var err error
	var fs *os.File
	m.db, err = memdb.Open(":memory:")
	if !util.IsExist(m.path) {
		return nil
	}
	fs, err = os.Open(m.path)
	if err != nil {
		return err
	}
	err = m.db.Load(fs)
	return err
}

func (m *memStorage) close() error {
	return m.db.Close()
}

func (m *memStorage) save() error {
	var (
		err  error
		file *os.File
	)
	file, err = os.Create(m.path)
	defer file.Close()
	if err != nil {
		return err
	}
	err = m.db.Save(file)
	if err != nil {
		return err
	}
	return err
}

func (m *memStorage) get(key string) (string, error) {

	var err error
	var text string
	err = m.db.View(func(tx *memdb.Tx) error {
		text, err = tx.Get(key)
		return err
	})

	return text, err
}

func (m *memStorage) set(key string, text string) error {
	var err error

	err = m.db.Update(func(tx *memdb.Tx) error {
		_, _, err := tx.Set(key, text, nil)
		return err
	})
	return err
}

func (m *memStorage) delete(key string) error {
	err := m.db.Update(func(tx *memdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
	return err
}

//流通道处理者
type dbNodeHandler struct {
	channel *network.StreamServer
	dbs     map[string]*memStorage
}

func newDBNodeHandler(id int, path string, names ...string) *dbNodeHandler {
	d := &dbNodeHandler{}
	d.dbs = make(map[string]*memStorage)
	cf := config.GetConfig()
	var i uint64
	for _, name := range names {
		for i = 1; i <= uint64(cf.KVDBMaxRange); i++ {
			dbName := name + "_" + strconv.FormatUint(i, 10)
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
	if !ok {
		result.ResultCode = config.MSG_KV_RESULT_FAILURE
		errMsg = fmt.Sprintf("数据库实例[%s]不存在", m.DBName)
		result.Text = errMsg
		d.channel.Send(result)
		return errors.New(errMsg)
	}
	key := strconv.FormatUint(m.Index, 10)

	switch m.Type {
	case config.MSG_KV_SET:
		err = db.set(key, m.Text)
		logger.Infof("数据库[%s]更新数据:[key:%d,text:%s]\n", m.DBName, m.Index, m.Text)

	case config.MSG_KV_GET:
		val, err = db.get(key)
		logger.Infof("数据库[%s]获取数据:[key:%d,text:%s]\n", m.DBName, m.Index, val)

	case config.MSG_KV_DEL:
		logger.Infof("数据库[%s]删除数据:[key:%d]\n", m.DBName, m.Index)
		err = db.delete(key)
	case config.MSG_KV_ClOSE:
		err = db.close()
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

type DBNode struct {
	channel *network.StreamServer
	peers   []string
}

func NewDBNode() (*DBNode, error) {
	c := config.GetCase()
	id := int(c.Local.ID)
	processor := newDBNodeHandler(id, config.GetConfig().KVDBFilePath, config.GetConfig().KVDBNames...)
	peers := c.GetUrls()
	channel, err := network.NewStreamServer(id, processor, peers...)
	if err != nil {
		return nil, err
	}
	node := &DBNode{}
	node.channel = channel
	processor.channel = channel
	return node, err
}
func NewProxyDBNode() (*DBNode, error) {
	c := config.GetCase()
	node := &DBNode{}
	node.peers = c.GetUrls()
	return node, nil
}
func (d *DBNode) Start() {
	d.channel.Start()
}
func (d *DBNode) StartProxy() {
	c := config.GetCase()
	go filedb.StartSequenceService()
	server := proxy.NewGrpcProxyServer(int(c.MasterCard.ID), d.peers, config.GetConfig().KVServerAddr)
	server.Start("INFO")
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

func (d *dbNodeClient) Get(key uint64, index uint64, item interface{}) (interface{}, error) {
	var err error
	var result *network.BatchMessage
	term, err := d.generateId()
	if err != nil {
		return nil, err
	}
	text := ""
	m := network.NewOnlyOneMsg(term, key, text, config.MSG_KV_GET)
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
	buff := util.StringToBytes(result.Messages[0].Text)
	err = json.Unmarshal(buff, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *dbNodeClient) generateId() (uint64, error) {

	node, err := utils.NewNode(0)
	if err != nil {
		return 0, err
	}
	id := uint64(node.Generate().Int64())
	return id, nil
}
func (d *dbNodeClient) Set(key uint64, index uint64, value interface{}) (error, uint64) {
	var result *network.BatchMessage
	buff, err := json.Marshal(value)
	if err != nil {
		return err, 0
	}
	text := util.BytesToString(buff)
	term, err := d.generateId()
	if err != nil {
		return err, 0
	}
	m := network.NewOnlyOneMsg(term, key, text, config.MSG_KV_SET)
	m.Messages[0].From = config.GetCase().GetMaster().ID
	m.Messages[0].To = d.Id
	m.Messages[0].DBName = d.dbName + "_" + strconv.FormatUint(index, 10)

	result, err = d.client.Send(m)
	if err != nil {
		return err, 0
	}
	if result == nil || len(result.Messages) == 0 {
		return errors.New(fmt.Sprintf("dbNodeClient Set操作失败[Key:%s]", key)), 0
	}
	resultMsg := result.Messages[0]

	if resultMsg.ResultCode == config.MSG_KV_RESULT_FAILURE {
		return errors.New(resultMsg.Text), 0
	}
	return nil, resultMsg.Index

}

func (d *dbNodeClient) Delete(key uint64, index uint64) error {
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
func NewKVStore(dbName string) (*shardedkv.KVStore, error) {
	cf := config.GetConfig()
	shards, err := newShards(dbName)
	if err != nil {
		return nil, err
	}
	chooser := shardedkv.NewRangeChooser(uint32(cf.KVDBMaxRange), uint32(cf.KVDBRowCount), uint32(cf.KVDRowStart))
	seq, err := filedb.NewSequenceProxy(cf.SequenceServer)
	if err != nil {
		return nil, err
	}
	kv := shardedkv.New(chooser, seq, shards)
	return kv, err
}
