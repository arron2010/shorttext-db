package memkv

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/memkv/proto"
	"sort"
)

type MemKVReducer struct {
}

func NewMemKVReducer() *MemKVReducer {
	m := &MemKVReducer{}
	return m
}

func (m *MemKVReducer) Reduce(sources map[int]*task.Task) (map[int]*task.Task, *task.TaskResult, error) {
	dbItems := make([]*proto.DbItem, 0, 4)
	list := &proto.DbItems{}
	for _, t := range sources {
		if t.Object == nil {
			continue
		}
		items := t.Object.(*proto.DbItems)
		dbItems = append(dbItems, items.Items...)
	}
	list.Items = dbItems
	sort.Sort(list)
	result := task.NewTaskResult(list)
	logger.Infof("数据库扫描汇总, 记录数:%d\n", len(list.Items))
	return sources, result, nil
}
