package proxy

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/errors"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/grpcpool"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/utils"
	"google.golang.org/grpc"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

var logger = glogger.MustGetLogger("proxy")

const (
	maxGRPCConnections = 100
	MASTER_NODE_ID     = 1
)

type NodeProxy struct {
	peers []string

	node     *network.StreamServer
	cache    *network.AsynCache
	Id       int
	sequence uint64
}

func NewNodeProxy(peers []string, logLevel string) *NodeProxy {
	glogger.SetModuleLevel("proxy", logLevel)
	network.SetLogLevel(logLevel)
	n := &NodeProxy{peers: peers, cache: network.NewMessageCache()}
	n.Id = MASTER_NODE_ID //节点标识
	node, err := network.NewStreamServer(n.Id, n, n.peers...)
	if err != nil {
		panic(err)
	}
	node.Start()
	n.node = node
	return n
}
func (n *NodeProxy) Send(batchMessage *network.BatchMessage) (*network.BatchMessage, error) {
	logger.Infof("--Proxy server received message term:%d GOROUTINE:%d\n", batchMessage.Term, utils.GetGID())
	//s.before(batchMessage)
	count := len(batchMessage.Messages)
	for i := 0; i < count; i++ {
		n.node.Send(*batchMessage.Messages[i])
	}
	result, err := n.cache.Get(batchMessage.Term, count)
	if err != nil {
		logger.Error("Proxy Server Failed:", err)
	}
	return result, err
}
func (n *NodeProxy) IsAlive(to uint64) bool {
	return n.node.IsAlive(int(to))
}

func (n *NodeProxy) SenMultiMsg(toList []uint64, op uint32, batchData [][]byte) ([][]byte, error) {
	var msg *network.Message
	var req *network.BatchMessage
	var resp *network.BatchMessage
	var err error
	l := len(toList)
	req = &network.BatchMessage{}
	req.Messages = make([]*network.Message, 0, l)

	for i := 0; i < l; i++ {
		msg = n.createMsg(toList[i], op, batchData[i])
		msg.Count = uint32(l)
		req.Term = msg.Term
		req.Messages = append(req.Messages, msg)
		logger.Infof("发送消息 From:%d To:%d Term:%d\n", msg.From, msg.To, msg.Term)
	}
	resp, err = n.Send(req)
	if err != nil {
		return nil, err
	}

	return n.getData(resp)
}

func (n *NodeProxy) SendSingleMsg(to uint64, op uint32, data []byte) ([]byte, error) {
	var msg *network.Message
	var err error
	var result *network.BatchMessage

	msg = n.createMsg(to, op, data)

	req := &network.BatchMessage{}
	req.Term = msg.Term
	req.Messages = []*network.Message{msg}

	logger.Infof("发送消息 From:%d To:%d Term:%d\n", msg.From, msg.To, msg.Term)
	result, err = n.Send(req)
	if err != nil {
		return nil, err
	}
	if len(result.Messages) > 0 {
		resp := result.Messages[0]
		logger.Infof("返回消息 From:%d To:%d Term:%d\n", resp.From, resp.To, resp.Term)

		if resp.ResultCode != config.MSG_KV_RESULT_SUCCESS {
			return nil, errors.New(fmt.Sprintf("远程服务器返回错误:%s op:%d", resp.Text, op))
		} else {
			return resp.Data, nil
		}
	} else {
		return nil, errors.New(fmt.Sprintf("远程服务器发生异常 op:%d", op))
	}
	return nil, nil

}
func (n *NodeProxy) Process(ctx context.Context, m network.Message) error {
	go func() {
		logger.Infof("++Proxy server begin to process message term:%d GOROUTINE:%d\n", m.Term, utils.GetGID())
		n.cache.Put(&m)
	}()
	return nil
}

func (n *NodeProxy) ReportUnreachable(id uint64) {

}

func (n *NodeProxy) generateId() uint64 {
	id := atomic.AddUint64(&n.sequence, 1)
	return id
}

func (n *NodeProxy) createMsg(to uint64, op uint32, data []byte) *network.Message {
	var msg *network.Message
	var term uint64
	term = n.generateId()
	msg = &network.Message{}
	msg.Term = term
	msg.Count = 1
	msg.Type = op
	msg.Data = data
	msg.From = uint64(n.Id)
	msg.To = to
	return msg
}
func (n *NodeProxy) getData(resp *network.BatchMessage) ([][]byte, error) {
	l := len(resp.Messages)
	errMsg := make([]string, 0, l)
	resultData := make([][]byte, 0, l)
	var err error
	for i := 0; i < l; i++ {
		if resp.Messages[i].ResultCode != config.MSG_KV_RESULT_SUCCESS {
			errMsg = append(errMsg, resp.Messages[i].Text)
		} else {
			resultData = append(resultData, resp.Messages[i].Data)
		}
	}
	if len(errMsg) > 0 {
		err = errors.New(strings.Join(errMsg, "\r\n"))
	}
	return resultData, err
}

type GrpcProxyServer struct {
	proxy *NodeProxy
	port  string
}

func NewGrpcProxyServer(peers []string, port string, logLevel string) *GrpcProxyServer {
	server := &GrpcProxyServer{}
	server.proxy = NewNodeProxy(peers, logLevel)
	server.port = port
	return server
}

func (s *GrpcProxyServer) Start() {
	go func() {
		lis, err := net.Listen("tcp", s.port)
		if err != nil {
			panic(err)
		}
		grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32), grpc.MaxSendMsgSize(math.MaxInt32))
		network.RegisterStreamProxyServer(grpcServer, s)
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

}

///*
// 代理服务器收到消息进行处理之前，进行预处理。
// */
//func (s *GrpcProxyServer)before(batchMessage *network.BatchMessage){
//	if len(batchMessage.Messages) > 0{
//		msg := batchMessage.Messages[0]
//		if msg.Type == config.MSG_KV_SET && msg.Index == 0{
//			msg.Index=s.seq.Next()
//		}
//	}
//}
func (s *GrpcProxyServer) Send(ctx context.Context, batchMessage *network.BatchMessage) (*network.BatchMessage, error) {

	return s.proxy.Send(batchMessage)
}

type StreamClient struct {
	timeout time.Duration
	pool    *grpcpool.Pool
}

func NewStreamClient(addr string, timeout time.Duration, idleTimeout time.Duration) (*StreamClient, error) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout/3)

	pool, err := grpcpool.NewWithContext(ctx, func(ctx context.Context) (*grpc.ClientConn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return grpc.Dial(addr, grpc.WithInsecure())
		}

	}, maxGRPCConnections, maxGRPCConnections, idleTimeout)

	if err != nil {
		return nil, err
	}
	s := &StreamClient{}
	s.pool = pool
	s.timeout = timeout
	defer cancel()
	return s, nil
}

func (s *StreamClient) Send(batchMessage *network.BatchMessage) (*network.BatchMessage, error) {

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	clientConn, err := s.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	client := network.NewStreamProxyClient(clientConn.ClientConn)
	result, err := client.Send(ctx, batchMessage)
	clientConn.Close()
	return result, err
}

func (s *StreamClient) Close() {
	s.pool.Close()
}
