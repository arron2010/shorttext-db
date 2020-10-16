package proxy

import (
	"context"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/grpcpool"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/utils"
	"google.golang.org/grpc"
	"math"
	"net"
	"time"
)

var logger = glogger.MustGetLogger("proxy")

const (
	maxGRPCConnections = 100
	MASTER_NODE_ID     = 1
)

type NodeProxy struct {
	peers []string

	node  *network.StreamServer
	cache *network.AsynCache
	Id    int
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

func (n *NodeProxy) Process(ctx context.Context, m network.Message) error {
	go func() {
		logger.Infof("++Proxy server begin to process message term:%d GOROUTINE:%d\n", m.Term, utils.GetGID())
		n.cache.Put(&m)
	}()
	return nil
}

func (n *NodeProxy) ReportUnreachable(id uint64) {

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
