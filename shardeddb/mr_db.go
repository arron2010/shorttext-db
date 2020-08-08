package shardeddb

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/utils"
	"strconv"
)

func LoadLookupJob(config *config.Config, stores map[string]IMemStorage) {
	var lookupJob *LookupJob
	var lookupMapper interfaces.IMapper
	var lookupConsumer interfaces.IConsumer
	var lookupReducer interfaces.IReducer

	lookupJob = &LookupJob{}
	lookupJob.config = config
	lookupJob.initialize()

	lookupMapper = &LookupMapper{}
	lookupConsumer = &LookupConsumer{dbs: stores}
	lookupReducer = &LookupReducer{}

	var jobHandler interfaces.IJobHandler = lookupJob

	easymr.Set(constants.JOB_HANDLER, jobHandler, "LookupJob")
	easymr.Set(constants.MAPPER, lookupMapper, "LookupMapper")
	easymr.Set(constants.CONSUMER, lookupConsumer, "LookupConsumer")
	easymr.Set(constants.REDUCER, lookupReducer, "LookupReducer")
}

type LookupJob struct {
	job    *task.Job
	config *config.Config
}

func (l *LookupJob) initialize() {

}
func (l *LookupJob) newJob(jobInfo *interfaces.JobInfo) *task.Job {
	job := task.MakeJob()
	tasks := l.createTasks(jobInfo)
	job.Tasks(tasks...)
	job.Stacks("LookupMapper", "LookupReducer")
	return job
}

func (l *LookupJob) createTasks(jobInfo *interfaces.JobInfo) []*task.Task {
	count := int(l.config.KVDBMaxRange)
	p := jobInfo.Source.(*findParam)
	tasks := make([]*task.Task, 0, count)
	for i := 1; i <= count; i++ {
		t := &task.Task{}
		t.Type = task.LONG
		t.Priority = task.BASE
		t.Consumable = "LookupConsumer"
		t.Result = task.Collection{}
		t.Stage = 0
		t.TimeOut = 0
		t.Object = &findParam{DBName: p.DBName + "_" + strconv.Itoa(i), Text: p.Text}
		t.Source = *task.NewCollection()
		t.Context = task.NewTaskContextEx()
		tasks = append(tasks, t)
	}

	return tasks
}

func (l *LookupJob) HandleJob(bg *task.Background, jobInfo *interfaces.JobInfo, context *task.TaskContext) {
	job := l.newJob(jobInfo)
	bg.Mount(job)
}

type LookupMapper struct {
}

func (l *LookupMapper) Map(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	result := task.NewTaskResult(struct{}{})
	return sources, result, nil
}

type LookupConsumer struct {
	dbs map[string]IMemStorage
}

func (l *LookupConsumer) Consume(workerId uint, taskItem *task.Task) bool {
	t := utils.NewTimer()
	p := taskItem.Object.(*findParam)
	logger.Info("LookupConsumer入参:", p)

	store, ok := l.dbs[p.DBName]
	result := task.NewTaskResult(make([]entities.Record, 0, 0))
	if !ok {
		result.Success = false
		logger.Errorf("Service:LookupConsumer,WorkerId:%d,GOROUTINE:%d,Time:%.2f,Message:%s数据库不存在\n", workerId, utils.GetGID(), t.Stop(), p.DBName)
		taskItem.Result.Append(result)
		return true
	}
	records, err := store.Find(p.Text)
	if err != nil {
		result.Success = false
		logger.Errorf("Service:LookupConsumer,WorkerId:%d,GOROUTINE:%d,Time:%.2fs,Message:查找%s|%\n", workerId, utils.GetGID(), t.Stop(), p.Text, err.Error())
		taskItem.Result.Append(result)
		return true
	}
	result.Content = records
	result.Success = true
	taskItem.Result.Append(result)
	logger.Infof("Service:LookupConsumer,WorkerId:%d,GOROUTINE:%d,Time:%.2f,Message:执行完成\n", workerId, utils.GetGID(), t.Stop())
	return true
}

type LookupReducer struct {
}

func (l *LookupReducer) Reduce(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	t := utils.NewTimer()
	all := make([]entities.Record, 0, 0)
	for _, t := range sources {
		for _, r := range t.Result {
			item := r.(*task.TaskResult)
			records := item.Content.([]entities.Record)
			all = append(all, records...)
		}
	}
	result := task.NewTaskResult(all)
	logger.Infof("Service:LookupReducer,GOROUTINE:%d,Time:%.2f,Message:汇总完成\n", utils.GetGID(), t.Stop())
	return sources, result, nil
}
