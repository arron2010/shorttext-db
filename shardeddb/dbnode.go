package shardeddb

import (
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/api"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/shardedkv"
	"strconv"
	"sync"
)

var dbNode *DBNode
var once sync.Once

type DBNode struct {
	channel *network.StreamServer
	peers   []string
	ID      int

	nodeHandler *dbNodeHandler
	chooser     api.Chooser
	config      *config.Config
}

type findParam struct {
	DBName string
	Text   string
}

func Start(remoting bool) {
	once.Do(func() {
		var err error
		dbNode, err = NewDBNode(remoting)
		if err != nil {
			panic(err)
		}
		if remoting {
			dbNode.Start()
			logger.Infof("服务器[%d]启动成功！", dbNode.ID)
		}

	})
}

func GetDBNode() *DBNode {
	return dbNode
}

func NewDBNode(remoting bool) (*DBNode, error) {

	var err error
	node := &DBNode{}
	c := config.GetCase()
	id := int(c.Local.ID)
	node.ID = id
	cfg := config.GetConfig()
	node.chooser = shardedkv.NewRangeChooser(
		uint32(config.GetConfig().KVDBMaxRange),
		uint32(config.GetConfig().KVDBRowCount),
		uint32(config.GetConfig().KVDRowStart))

	cards := config.GetCase().CardList
	shardNames := make([]string, 0, len(cards))
	for i := 0; i < len(cards); i++ {
		shardNames = append(shardNames, cards[i].Name)
	}
	node.chooser.SetBuckets(shardNames)

	processor := newDBNodeHandler(id,
		int(config.GetConfig().KVDBMaxRange),
		config.GetConfig().KVDBFilePath,
		config.GetConfig().KVDBNames...)

	if remoting {
		var channel *network.StreamServer
		peers := c.GetUrls()
		channel, err := network.NewStreamServer(id, processor, peers...)
		if err != nil {
			return nil, err
		}
		node.channel = channel
		processor.channel = channel
	}
	processor.clbt = collaborator.NewCollaborator(int(cfg.KVDBMaxRange))
	processor.defaultDB = cfg.KVDBNames[0]
	node.nodeHandler = processor
	LoadLookupJob(cfg, processor.dbs)

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

func (d *DBNode) GetMemStorage(dbName string) IMemStorage {
	return d.nodeHandler.dbs[dbName]
}

func (d *DBNode) StartProxy() {
	//c := config.GetCase()
	go filedb.StartSequenceService()
	server := proxy.NewGrpcProxyServer(d.peers, "5009", config.GetConfig().KVServerAddr)
	server.Start()
}

func (d *DBNode) Find(db string, text string) ([]entities.Record, error) {
	return d.nodeHandler.find(db, text)
}

func (d *DBNode) GetCount(db string) []int {
	return d.nodeHandler.getCount(db)
}
func (d *DBNode) Set(dbName string, key uint64, value string) (err error) {
	shardName, index := d.chooser.Choose(key)
	if config.GetCase().Local.Name != shardName {
		return errors.New(fmt.Sprintf("分片名称不同, local:%s,actual:%s", config.GetCase().Local.Name, shardName))
	}
	actualDb := dbName + "_" + strconv.FormatUint(uint64(index), 10)
	store, ok := d.nodeHandler.dbs[actualDb]
	if !ok {
		return errors.New(fmt.Sprintf("数据库不存在, DbName:%s", actualDb))
	}
	strKey := strconv.FormatUint(key, 10)
	err = store.SetWithIndex(strKey, value, config.GJSON_FIELD_DESC)

	return err
}
