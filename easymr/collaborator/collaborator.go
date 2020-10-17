package collaborator

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/iexecutor"
	"github.com/xp/shorttext-db/easymr/artifacts/iworkable"
	"github.com/xp/shorttext-db/easymr/artifacts/master"
	"github.com/xp/shorttext-db/easymr/artifacts/stats"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/easymr/grpc/proto"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/easymr/store"
	"github.com/xp/shorttext-db/glogger"
	"github.com/xp/shorttext-db/network/proxy"
	"sync"
	"time"
)

const (
	MAX_IDLE_CONNECTIONS int           = 20
	REQUEST_TIMEOUT      time.Duration = 5
	UPDATE_INTERVAL      time.Duration = 1
)

var once sync.Once
var singleton *Collaborator

var logger = glogger.MustGetLogger("collaborator")
var firstStarted = false

type LocalHandler func(sources map[int]*task.Task) (map[int]*task.Task, error)

/*
mapreduce服务启动者，启动GRPC服务以及HTTP服务
GRPC服务负责节点之间通讯，HTTP服务用来发布对外调用
*/
type Collaborator struct {
	Workable   iworkable.Workable
	masterNode *ClbtMasterNode
	slaveNode  *ClbtSlaveNode
}

func GetCollaborator() *Collaborator {
	once.Do(func() {
		singleton = NewCollaborator(config.GetConfig().WorkerPerMaster)

	})
	return singleton
}
func NewCollaborator(amount int) *Collaborator {

	mst := master.NewMaster()
	mst.BatchAttach(amount)
	mst.LaunchAll()

	clbt := &Collaborator{Workable: mst}
	if config.GetCase().Local.ID == proxy.MASTER_NODE_ID {
		clbt.masterNode = newClbtMasterNode()
	} else {
		clbt.slaveNode = newClbtSlaveNode(clbt.Workable)

	}
	return clbt
}

func (clbt *Collaborator) Join(wk iworkable.Workable) {
	clbt.Workable = wk
}

func (clbt *Collaborator) Clean() {
	//cardHelper.RangePrint(cards)
}

func (clbt *Collaborator) Handle(router *mux.Router) *mux.Router {

	return router
}
func (clbt *Collaborator) DistributeSeq(sources map[int]*task.Task, localTask bool) (map[int]*task.Task, error) {
	if localTask {
		return clbt.localDistributeSeq(sources)
	} else {
		return clbt.remoteDistributeSeq(sources)
	}
}

func (clbt *Collaborator) remoteDistributeSeq(sources map[int]*task.Task) (map[int]*task.Task, error) {
	return clbt.masterNode.broadcast(sources, clbt.localDistributeSeq)
}

func (clbt *Collaborator) localDistributeSeq(sources map[int]*task.Task) (map[int]*task.Task, error) {

	var (
		result  map[int]*task.Task = make(map[int]*task.Task)
		counter int                = 0
		handled                    = 0
	)

	chs := make(map[int]chan *task.Task)
	for k, v := range sources {
		//如果任务为本地执行，将不分发到其他服务器
		p := *v
		chs[k] = clbt.DelayExecute(&p)
		logger.Infof("++完成本地处理，任务序号:%d\n", k)
		handled++
	}
	for {
		for i, ch := range chs {
			select {
			case t := <-ch:
				result[i] = t
				counter++
			}
		}
		if counter >= handled {
			break
		}
	}
	logger.Infof("完成任务处理 任务总数量:%d 完成任务数量:%d\n", handled, counter)
	return result, nil
}

func (clbt *Collaborator) printTask(ip string, t *task.Task) {
	//if t.Consumable =="BatchConsumer"{
	//	logger.Infof("task content=%s ip address=%s\n",t.ToString(),ip)
	//}
}
func (clbt *Collaborator) ExecuteJobHandler(msg *proto.ReqMRMsg) (*proto.RespMRMsg, error) {

	return nil, nil
}

func (clbt *Collaborator) SharedDistribute(pmaps *map[int]*task.Task, stacks []string, localTask bool) (*task.TaskResult, error) {

	sm := stats.GetStatsInstance()
	sm.Record("tasks", len(*pmaps))
	var (
		err    error
		fs     = store.GetInstance()
		maps   = *pmaps
		result *task.TaskResult
	)

	for _, stack := range stacks {
		var (
			exe iexecutor.IExecutor
		)
		exe, err = fs.GetExecutor(stack)

		if err != nil {
			return nil, err
		}

		switch exe.Type() {
		case constants.EXECUTOR_TYPE_MAPPER:
			maps, _, err = exe.Execute(maps)
			if err != nil {
				return nil, err
			}
			maps, err = clbt.DistributeSeq(maps, localTask)
		case constants.EXECUTOR_TYPE_REDUCER:
			maps, result, err = exe.Execute(maps)
			if err != nil {
				return nil, err
			}
		default:
			maps, result, err = exe.Execute(maps)
			if err != nil {
				return nil, err
			}
		}
	}
	*pmaps = maps

	return result, nil
}

func (clbt *Collaborator) DelayExecute(t *task.Task) chan *task.Task {
	ch := make(chan *task.Task)

	go func() {
		defer close(ch)
		err := clbt.Workable.Done(t)
		if err != nil {
			ch <- task.NewErrorTask(err.Error())
		} else {
			ch <- t
		}
	}()

	return ch
}

func Delay(sec time.Duration) {
	tm := time.NewTimer(sec * time.Second)
	<-tm.C
}

func printProgress(num interface{}, tol interface{}) {
	//logger.Infof("远程任务响应[" + fmt.Sprintf("%v", num) + "/" + fmt.Sprintf("%v", tol) + "]")
}

func (clbt *Collaborator) MapReduce(jobInfo *interfaces.JobInfo, context *task.TaskContext) (*task.TaskResult, error) {

	var err error
	fs := store.GetInstance()
	var (
		bg         = task.NewBackground()
		jobHandler interfaces.IJobHandler
		counter    = 0
	)
	jobHandler = fs.GetJobHandler(jobInfo.Handler)
	jobHandler.HandleJob(bg, jobInfo, context)

	var taskResult *task.TaskResult

	job := bg.Done()
	defer bg.Close()
	if job.Len() == 0 {
		err = errors.New(fmt.Sprintf("任务为空，设置的处理阶段为%s", job.GetStacks()))
		return taskResult, err
	}
	for s := job.Front(); s != nil; s = s.Next() {
		exes, err := job.Exes(counter)
		if err != nil {
			logger.Error(err)
			break
		}
		taskResult, err = clbt.SharedDistribute(&s.TaskSet, exes, jobInfo.LocalJob)
		counter++
	}

	if err != nil {
		taskResult = task.NewTaskResult(struct{}{})
		taskResult.Success = false
		taskResult.Message = err.Error()
		delete(context.Context, constants.JOB_RESULT)
	} else {
		context.Context[constants.JOB_RESULT] = taskResult
	}
	return taskResult, err
}
