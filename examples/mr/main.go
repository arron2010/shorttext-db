package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/shardeddb"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case1.txt", func(config *config.Config) {
		shardeddb.LoadLookupJob(config)
	})
	clbt := collaborator.NewCollaborator(3)
	jobInfo := &interfaces.JobInfo{}
	jobInfo.Handler = "LookupJob"
	jobInfo.Params = make(map[string]string)
	jobInfo.Context = make(map[string][]byte)
	jobInfo.LocalJob = true
	context := &task.TaskContext{}
	context.Context = make(map[string]interface{})
	clbt.MapReduce(jobInfo, context)
}
