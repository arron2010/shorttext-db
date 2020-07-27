package shardeddb

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"sync"
)

var dbNode *DBNode
var once sync.Once

type DBNode struct {
	channel *network.StreamServer
	peers   []string
	ID      int
	dbs     map[string]IMemStorage
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
		}
		logger.Infof("服务器[%d]启动成功！", dbNode.ID)
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

	processor := newDBNodeHandler(id, config.GetConfig().KVDBFilePath, config.GetConfig().KVDBNames...)

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

	node.dbs = processor.dbs
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
	return d.dbs[dbName]
}

func (d *DBNode) StartProxy() {
	c := config.GetCase()
	go filedb.StartSequenceService()
	server := proxy.NewGrpcProxyServer(int(c.MasterCard.ID), d.peers, config.GetConfig().KVServerAddr)
	server.Start("INFO")
}
