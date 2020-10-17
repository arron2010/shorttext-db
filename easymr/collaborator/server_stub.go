package collaborator

import (
	"errors"
	"github.com/xp/shorttext-db/easymr/artifacts/iworkable"
	"github.com/xp/shorttext-db/easymr/artifacts/message"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	. "github.com/xp/shorttext-db/easymr/collaborator/services"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math"
	"net"
	"time"
)

//var blockCache *task.BlockCache

func NewServiceServerStub(wk iworkable.Workable) *ServiceServerStub {
	return &ServiceServerStub{wk}
}

type ServiceServerStub struct {
	workable iworkable.Workable
}

//launch Gossip server using gprc
func LaunchServer(addr string, wkb iworkable.Workable) {
	//	blockCache = task.NewBlockCache(utils.GetSettings().CacheServerAddr, utils.GetSettings().CacheServerPassword)
	go func() {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}
		grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32), grpc.MaxSendMsgSize(math.MaxInt32))
		RegisterRPCServiceServer(grpcServer, &ServiceServerStub{wkb})
		grpcServer.Serve(lis)
	}()
}

func (stub *ServiceServerStub) Exchange(
	ctx context.Context,
	in *message.CardMessage) (*message.CardMessage, error) {

	var out *message.CardMessage
	out = message.NewCardMessage()
	if len(in.From.IP) > 0 {
		out.Status = &message.Status{Key: "timestamp", Value: time.Now().Format("2006-01-02 15:04:05")}
		return out, nil
	} else {
		return out, errors.New("无法获取对方IP地址级及端口")
	}

	//out, err = messageHelper.Exchange(in)

	//if !util.IsNil(in){
	//	out, err = messageHelper.Exchange(in)
	//	logger.Debugf("server(%s:%d) message received from %s:%d\n",in.To.IP,in.To.Port,in.From.IP,in.From.Port)
	//}else{
	//	err = errors.New("无法获取对方IP地址级及端口")
	//}
	return out, nil
}

// Local implementation of rpc method Distribute()
func (stub *ServiceServerStub) Distribute(
	ctx context.Context,
	in *task.TaskPayload) (out *task.TaskPayload, err error) {

	//var (
	//	src *map[int]*task.Task
	//	rst *map[int]*task.Task
	//)
	////	fs := store.GetInstance()
	////	fs.TypeRegister.RegisterTypes()
	//
	//src, err = Decode(in, task.SOURCE_SERIALIZE)
	//
	////for _, v := range *src {
	////	v.BinaryContent = in.BigPayload
	////}
	//
	//rst, err = stub.distribute(src)
	//out, err = Encode(rst, task.RESULT_SERIALIZE)

	return out, err
}

// Local implementation of rpc method Distribute()
func (stub *ServiceServerStub) distribute(
	source *map[int]*task.Task) (*map[int]*task.Task, error) {
	var (
		result = new(map[int]*task.Task)
		err    error
	)

	s := *source

	//for k, v := range s {
	//	if v.RunType > 0 {
	//		_, err := blockCache.Get(uint32(v.RunType), k)
	//		if err != nil {
	//			logger.Errorf("任务（%d）获取二进制数据块失败:%s\n", k, err)
	//		}
	//		//logger.Infof("服务器开始任务处理，数据块大小:%d\n",len(data))
	//	}
	//}

	err = stub.workable.DoneMulti(s)
	*result = s
	return result, err
}
