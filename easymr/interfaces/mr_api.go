package interfaces

import "github.com/xp/shorttext-db/easymr/artifacts/task"

type JobInfo struct {
	Handler  string
	Mapper   string
	Reducer  string
	Consumer string
	Params   map[string]string
	Context  map[string][]byte
	Source   interface{}
	/*用于执行多次mapreduce*/
	Next     *JobInfo
	LocalJob bool
}

func NewSimpleJobInfo(handler string, local bool, src interface{}) *JobInfo {
	jobInfo := &JobInfo{}
	jobInfo.Handler = handler
	jobInfo.Params = make(map[string]string)
	jobInfo.Context = make(map[string][]byte)
	jobInfo.LocalJob = local
	jobInfo.Source = src
	return jobInfo
}

/*
配置需要执行的mapreduce的任务
*/
type IJobHandler interface {
	HandleJob(bg *task.Background, jobInfo *JobInfo, context *task.TaskContext)
}

type IMapper interface {
	Map(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error)
}

/*
消费者接口，用来处理mapper以后的数据
*/
type IConsumer interface {
	Consume(workerId uint, taskItem *task.Task) bool
}

type IReducer interface {
	Reduce(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error)
}
