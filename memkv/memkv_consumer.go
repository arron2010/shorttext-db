package memkv

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/memkv/proto"
	"github.com/xp/shorttext-db/utils"
)

type MemKVConsumer struct {
	db MemDB
}

func NewMemKVConsumer(db MemDB) *MemKVConsumer {
	m := &MemKVConsumer{}
	m.db = db
	return m
}

func (m *MemKVConsumer) Consume(workerId uint, taskItem *task.Task) bool {
	if utils.IsNil(m.db) {
		taskItem.Object = nil
		return true
	}
	if taskItem.Object == nil {
		return true
	}
	queryParam := taskItem.Object.(*proto.DbQueryParam)

	data := m.db.Scan(queryParam.StartKey, queryParam.EndKey)
	taskItem.Object = data
	logger.Infof("完成数据库处理,记录数:%d\n", len(data.Items))

	return true
}
