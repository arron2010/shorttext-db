package proxy

import (
	"com.neep/goplatform/util"
	"context"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/network"
	"google.golang.org/grpc"
	"math"
	"net"
)

var logger = glogger.MustGetLogger("proxy")

type GrpcProxyServer struct {
	peers []string
	port  string
	node  *network.StreamServer
	cache *network.AsynCache
}

func NewGrpcProxyServer(peers []string, port string) *GrpcProxyServer {
	server := &GrpcProxyServer{peers: peers, port: port, cache: network.NewMessageCache()}
	return server
}

func (s *GrpcProxyServer) Start(logLevel string) {
	glogger.SetModuleLevel("proxy", logLevel)
	network.SetLogLevel(logLevel)

	node, err := network.NewStreamServer(1, s, s.peers...)
	if err != nil {
		panic(err)
	}
	node.Start()
	s.node = node

	lis, err := net.Listen("tcp", s.port)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32), grpc.MaxSendMsgSize(math.MaxInt32))
	network.RegisterStreamProxyServer(grpcServer, s)

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

func (s *GrpcProxyServer) Send(ctx context.Context, batchMessage *network.BatchMessage) (*network.BatchMessage, error) {
	logger.Infof("--Proxy server received message term:%d GOROUTINE:%d\n", batchMessage.Term, util.GetGID())
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
		logger.Infof("++Proxy server begin to process message term:%d GOROUTINE:%d\n", m.Term, util.GetGID())
		s.cache.Put(&m)
	}()

	return nil
}

func (s *GrpcProxyServer) ReportUnreachable(id uint64) {

}
