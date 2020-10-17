package collaborator

import (
	"github.com/xp/shorttext-db/easymr/artifacts/card"
	"github.com/xp/shorttext-db/easymr/artifacts/message"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	. "github.com/xp/shorttext-db/easymr/collaborator/services"
	"github.com/xp/shorttext-db/easymr/constants"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ServiceClientStub struct {
	RPCServiceClient
	clientConn    *grpc.ClientConn
	clientContact card.Card
}

func NewServiceClientStubEx() *ServiceClientStub {
	var stub *ServiceClientStub = &ServiceClientStub{}
	return stub
}

func NewServiceClientStub(endpoint string, port int32, secure ...bool) (stub *ServiceClientStub, err error) {
	if len(secure) < 1 {
		clientContact := card.Card{endpoint, port, true, "", false}
		//	grpc.WithTimeout(constants.DEFAULT_RPC_DIAL_TIMEOUT),
		ctx, cancel := context.WithTimeout(context.Background(), constants.DEFAULT_RPC_DIAL_TIMEOUT)
		conn, err := grpc.DialContext(ctx,
			clientContact.GetFullIP(),
			grpc.WithInsecure(),
			grpc.WithBlock())
		defer cancel()

		if err != nil {
			logger.Errorf("Dialing:%s,IP:%s", err, clientContact.GetFullIP())
			return &ServiceClientStub{}, err
		}

		return &ServiceClientStub{NewRPCServiceClient(conn), conn, clientContact}, nil
	}
	// todo: change return to TLS client
	return &ServiceClientStub{}, nil
}

func (stub *ServiceClientStub) DistributeAsyncEx(source *map[int]*task.Task) chan *task.Task {
	ch := make(chan *task.Task)
	return ch
}

func (stub *ServiceClientStub) DistributeAsync(source *map[int]*task.Task, ip string) chan *task.Task {
	ch := make(chan *task.Task)

	//go func() {
	//	defer close(ch)
	//	var err error
	//	var enc *task.TaskPayload
	//	begin := time.Now()
	//	enc, err = Encode(source, task.SOURCE_SERIALIZE)
	//	if err != nil {
	//		logger.Errorf("在远程服务器[%s]处理之前，序列化发生错误:%s\n", ip, err.Error())
	//		return
	//	}
	//	var dec *task.TaskPayload
	//	dec, err = stub.RPCServiceClient.Distribute(context.Background(), enc, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	//	if err != nil {
	//		logger.Errorf("远程服务器[%s]调用错误:%s\n", ip, err.Error())
	//		return
	//	}
	//	result, err := Decode(dec, task.RESULT_SERIALIZE)
	//	if err != nil {
	//		logger.Errorf("DistributeAsync decode erro:%s", err)
	//		return
	//	}
	//	for _, t := range *result {
	//		ch <- t
	//	}
	//	elapsed := time.Since(begin)
	//	logger.Infof("远程服务器[%s]完成处理，消耗时间%.2f\n", ip, elapsed)
	//}()
	return ch
}

func (stub *ServiceClientStub) Exchange(in *message.CardMessage) (*message.CardMessage, error) {
	return stub.RPCServiceClient.Exchange(context.Background(), in)
}

func (stub *ServiceClientStub) Close() {
	if stub.clientConn != nil {
		stub.clientConn.Close()
	}
}
