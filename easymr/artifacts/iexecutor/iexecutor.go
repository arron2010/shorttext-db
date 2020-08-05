package iexecutor

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
)

type IExecutor interface {
	Execute(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error)
	Todo(todo func(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error))
	Type(t ...string) string
}

func Default() *DefaultExecutor {
	return new(DefaultExecutor)
}

type DefaultExecutor struct {
	todo  func(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error)
	_type string
}

func (exe *DefaultExecutor) Type(t ...string) string {
	if len(t) > 0 {
		exe._type = t[0]
	}
	return exe._type
}

func (exe *DefaultExecutor) Todo(todo func(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error)) {
	exe.todo = todo
}

func (exe *DefaultExecutor) Execute(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	return exe.todo(sources)
}
