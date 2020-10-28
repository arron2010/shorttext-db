package memkv

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/memkv/proto"
)

type MemKVJob struct {
}

func NewMemKVJob() *MemKVJob {
	m := &MemKVJob{}
	return m
}
func (m *MemKVJob) HandleJob(bg *task.Background, jobInfo *interfaces.JobInfo, context *task.TaskContext) {
	job := m.newJob(jobInfo)
	bg.Mount(job)
}

func (m *MemKVJob) newJob(jobInfo *interfaces.JobInfo) *task.Job {
	job := task.MakeJob()
	tasks := m.createTasks(jobInfo)
	job.Tasks(tasks...)
	job.Stacks("MemKVMapper", "MemKVReducer")
	return job
}

func (m *MemKVJob) createTasks(jobInfo *interfaces.JobInfo) []*task.Task {
	count := config.GetConfig().WorkerPerMaster
	tasks := make([]*task.Task, 0, count)
	for i := 1; i <= count; i++ {
		t := &task.Task{}
		t.Type = task.LONG
		t.Priority = task.BASE
		t.Consumable = "MemKVConsumer"
		t.Result = task.Collection{}
		t.Stage = 0
		t.Object = jobInfo.Source.(*proto.DBQueryParam)
		t.Source = *task.NewCollection()
		t.Context = task.NewTaskContextEx()
		tasks = append(tasks, t)
	}

	return tasks
}
