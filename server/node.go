package server

import (
	"context"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"sync"
)

var logger = glogger.MustGetLogger("server")

type Handler interface {
	Handle(msgType uint32, data []byte) ([]byte, bool, error)
}

var once sync.Once
var gNode *Node
var gNodeProxy *proxy.NodeProxy

type Node struct {
	handlers []Handler
	channel  *network.StreamServer
}

func GetNodeProxy() *proxy.NodeProxy {
	once.Do(func() {
		c := config.GetCase()
		gNodeProxy = proxy.NewNodeProxy(c.GetUrls(), config.GetConfig().LogLevel)
	})
	return gNodeProxy
}

func GetNode() *Node {
	once.Do(func() {
		var err error
		gNode = &Node{}
		c := config.GetCase()
		id := int(c.Local.ID)
		peers := c.GetUrls()
		gNode.channel, err = network.NewStreamServer(id, gNode, peers...)
		if err != nil {
			panic(err)
		}
		gNode.handlers = make([]Handler, 0, 8)
		gNode.channel.Start()
	})
	return gNode
}

func (n *Node) Process(ctx context.Context, m network.Message) error {
	var err error
	var data []byte
	var used bool
	//logger.Infof("收到消息 From:%d To:%d Term:%d\n", m.From, m.To, m.Term)
	for _, h := range n.handlers {

		data, used, err = h.Handle(m.Type, m.Data)
		if !used {
			continue
		}
		result := network.Message{}
		result.To = m.From
		result.From = m.To
		result.Count = m.Count
		result.Term = m.Term
		result.ResultCode = config.MSG_KV_RESULT_SUCCESS
		result.Index = m.Index
		if err != nil {
			result.ResultCode = config.MSG_KV_RESULT_FAILURE
			result.Text = err.Error()
		}
		result.Data = data
		n.channel.Send(result)
		logger.Infof("回复消息 From:%d To:%d Term:%d Type:%d\n", result.From, result.To, result.Term, m.Type)
	}

	return err
}

func (n *Node) ReportUnreachable(id uint64) {

}

func (n *Node) RegisterHandler(h Handler) {
	n.handlers = append(n.handlers, h)
}
