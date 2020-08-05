package proxy

import (
	"context"
	"github.com/xp/shorttext-db/filedb"
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
)

type GrpcProxyServer struct {
	peers []string
	port  string
	node  *network.StreamServer
	cache *network.AsynCache
	Id    int
	seq   *filedb.Sequence
}

func NewGrpcProxyServer(id int, peers []string, port string) *GrpcProxyServer {
	server := &GrpcProxyServer{peers: peers, port: port, cache: network.NewMessageCache()}
	server.Id = id

	return server
}

func (s *GrpcProxyServer) Start(logLevel string) {
	glogger.SetModuleLevel("proxy", logLevel)
	network.SetLogLevel(logLevel)

	node, err := network.NewStreamServer(s.Id, s, s.peers...)
	if err != nil {
		panic(err)
	}
	node.Start()
	s.node = node

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

	logger.Infof("--Proxy server received message term:%d GOROUTINE:%d\n", batchMessage.Term, utils.GetGID())
	//s.before(batchMessage)
	count := len(batchMessage.Messages)
	for i := 0; i < count; i++ {
		s.node.Send(*batchMessage.Messages[i])
	}
	result, err := s.cache.Get(batchMessage.Term, count)
	if err != nil {
		logger.Error("Proxy Server Failed:", err)
	}
	return result, err
}

func (s *GrpcProxyServer) Process(ctx context.Context, m network.Message) error {
	go func() {
		logger.Infof("++Proxy server begin to process message term:%d GOROUTINE:%d\n", m.Term, utils.GetGID())
		s.cache.Put(&m)
	}()

	return nil
}

func (s *GrpcProxyServer) ReportUnreachable(id uint64) {

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
