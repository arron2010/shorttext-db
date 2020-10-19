package memkv

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
)

type MemKVMapper struct {
}

func NewMemKVMapper() *MemKVMapper {
	m := &MemKVMapper{}
	return m
}

func (l *MemKVMapper) Map(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	result := task.NewTaskResult(struct{}{})

	return sources, result, nil
}
