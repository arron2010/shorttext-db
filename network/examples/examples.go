package examples

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/utils"
)

var _ network.Processor = (*DemoProcessor)(nil)

type DemoProcessor struct {
	streamServer *network.StreamServer
}

func NewDemoProcessor() *DemoProcessor {
	return &DemoProcessor{}
}

func (demo *DemoProcessor) Process(ctx context.Context, m network.Message) error {
	fmt.Printf("已收到 term:%d content:%s goroutine:%d\n", m.Term, m.Text, utils.GetGID())
	newMsg := network.Message{}
	newMsg.To = m.From
	newMsg.From = m.To
	newMsg.Term = m.Term
	newMsg.Count = m.Count
	newMsg.Text = fmt.Sprintf("【%d】已处理", utils.GetGID())
	demo.streamServer.Send(newMsg)
	return nil
}

func (demo *DemoProcessor) ReportUnreachable(id uint64) {
	fmt.Println("消息处理失败 节点---->", id)
}

func StartStreamServer(id int) {
	var peerUrls = []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003", "http://127.0.0.1:8004"}
	p := NewDemoProcessor()
	streamServer, err := network.NewStreamServer(id, p, peerUrls...)
	p.streamServer = streamServer
	if err != nil {
		fmt.Println(err)
		return
	}
	streamServer.Start()
	utils.WaitFor()
}

func StartProxyServer() {
	var peerUrls = []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003", "http://127.0.0.1:8004"}
	server := proxy.NewGrpcProxyServer(peerUrls, ":5009", "DEBUG")
	server.Start()
	utils.WaitFor()
}
