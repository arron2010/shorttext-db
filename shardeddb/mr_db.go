package shardeddb

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/utils"
)

func LoadLookupJob(config *config.Config) {
	var lookupJob *LookupJob
	var lookupMapper interfaces.IMapper
	var lookupConsumer interfaces.IConsumer
	var lookupReducer interfaces.IReducer

	lookupJob = &LookupJob{}
	lookupJob.config = config
	lookupJob.initialize()

	lookupMapper = &LookupMapper{}
	lookupConsumer = &LookupConsumer{}
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
	l.job = l.newJob()
}
func (l *LookupJob) newJob() *task.Job {
	job := task.MakeJob()
	tasks := l.createTasks()
	job.Tasks(tasks...)
	job.Stacks("LookupMapper", "LookupReducer")
	return job
}

func (l *LookupJob) createTasks() []*task.Task {
	count := int(l.config.KVDBMaxRange)
	tasks := make([]*task.Task, 0, count+2)
	for i := 0; i < count; i++ {
		t := &task.Task{}
		t.Type = task.LONG
		t.Priority = task.BASE
		t.Consumable = "LookupConsumer"
		t.Result = task.Collection{}
		t.Stage = 0
		t.TimeOut = 0
		t.Source = *task.NewCollection()
		t.Context = task.NewTaskContextEx()
		tasks = append(tasks, t)
	}

	return tasks
}

func (l *LookupJob) HandleJob(bg *task.Background, jobInfo *interfaces.JobInfo, context *task.TaskContext) {
	bg.Mount(l.job)
}

type LookupMapper struct {
}

func (l *LookupMapper) Map(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	result := task.NewTaskResult(struct{}{})
	fmt.Println("LookupMapper---------------->")
	return sources, result, nil
}

type LookupConsumer struct {
}

func (l *LookupConsumer) Consume(source *task.Collection, result *task.Collection, context *task.TaskContext) bool {
	fmt.Printf("LookupConsumer----------------> %d \n", utils.GetGID(), context.Context[constants.WORKER_ID])
	return true
}

type LookupReducer struct {
}

func (l *LookupReducer) Reduce(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	fmt.Println("LookupReducer---------------->")
	result := task.NewTaskResult(struct{}{})
	return sources, result, nil
}
