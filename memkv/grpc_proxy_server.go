package memkv

import (
	"context"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/network"
	"github.com/xp/shorttext-db/server"
	"google.golang.org/grpc"
	"math"
	"net"
)

type Handler func(proxy *RemoteDBProxy)
type GrpcProxyServer struct {
	proxy   *RemoteDBProxy
	Handler Handler
}

func NewGrpcProxyServer() *GrpcProxyServer {
	s := &GrpcProxyServer{}
	s.proxy = NewRemoteDBProxy(server.GetNodeProxy(), collaborator.GetCollaborator())
	return s
}

func (s *GrpcProxyServer) Start() {
	go func() {
		lis, err := net.Listen("tcp", ":5009")
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
func (s *GrpcProxyServer) Send(ctx context.Context, batchMessage *network.BatchMessage) (*network.BatchMessage, error) {
	logger.Info("--开始处理")
	s.Handler(s.proxy)
	logger.Info("++完成处理")
	return nil, nil
}
