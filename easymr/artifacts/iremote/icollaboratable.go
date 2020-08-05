package iremote

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
)

type ICollaboratable interface {
	SyncDistribute(sources []*task.Task) ([]*task.Task, error)
}
