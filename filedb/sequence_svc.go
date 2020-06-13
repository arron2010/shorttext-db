package filedb

import (
	"context"
	"google.golang.org/grpc"
	"net"
	"time"
)

type SequenceService struct {
	seq *Sequence
}

func StartSequenceService() {
	var err error
	s := &SequenceService{}
	s.seq = NewSequence(0)

	lis, err := net.Listen("tcp", ":7892")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	RegisterSequenceServer(grpcServer, s)
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}

}

func (s *SequenceService) Next(ctx context.Context, msg *SequenceMsg) (*SequenceMsg, error) {
	next := s.seq.Next()
	logger.Info("生成序列值:", next)
	return &SequenceMsg{Next: next}, nil

}

func (s *SequenceService) Start(ctx context.Context, msg *SequenceMsg) (*SequenceMsg, error) {
	err := s.seq.SetStart(msg.Start)
	logger.Info("初始化序列值:", msg.Start)
	return msg, err
}

type SequenceProxy struct {
	conn   *grpc.ClientConn
	client SequenceClient
}

func NewSequenceProxy(addr string) (*SequenceProxy, error) {
	s := &SequenceProxy{}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	s.conn = conn
	if err != nil {
		return nil, err
	}
	client := NewSequenceClient(conn)
	s.client = client
	return s, nil
}

func (s *SequenceProxy) Next() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	result, err := s.client.Next(ctx, &SequenceMsg{})
	if err != nil {
		logger.Error("获取自增序列失败", err)
		return 0
	}
	return result.Next
}

func (s *SequenceProxy) SetStart(val uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	msg := &SequenceMsg{}
	msg.Start = val
	_, err := s.client.Start(ctx, msg)
	return err
}

func (s *SequenceProxy) Close() {
	s.conn.Close()
}
