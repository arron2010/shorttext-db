package task

import (
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/utils"
	"sync"
)

type TaskContext struct {
	Context map[string]interface{}
	lock    sync.RWMutex
	Term    int
	Index   int
}

func NewTaskContext(ctx interface{}) *TaskContext {
	maps := utils.Map(ctx)
	return &TaskContext{maps, sync.RWMutex{}, 0, 0}
}

func NewTaskContextEx() *TaskContext {
	maps := make(map[string]interface{})
	return &TaskContext{maps, sync.RWMutex{}, 0, 0}
}

/*
由于TaskContext在Task对象中作为引用类型，在分拆任务时，最好先克隆一个副本，保证线程安全
*/
func (this *TaskContext) Clone() *TaskContext {
	newObj := &TaskContext{}
	newObj.Context = make(map[string]interface{})
	if this.Context != nil {
		for k, v := range this.Context {
			newObj.Context[k] = v
		}
	}
	newObj.Index = this.Index
	newObj.Term = this.Term
	return newObj
}
func (this *TaskContext) SetIndex(index int) {
	//logger.Infof("****test v.Context.Index-->%d index-->%d addr-->%p goroutine -->%d\n",this.Index,index,this,util.GetGID())
	this.Index = index
}

func (this *TaskContext) Entries() map[string]interface{} {
	return this.Context
}

func (this *TaskContext) Set(key string, val interface{}) {
	this.lock.Lock()
	this.Context[key] = val
	this.lock.Unlock()
}

func (this *TaskContext) Get(key string) (interface{}, error) {
	if val := this.Context[key]; val != nil {
		return val, nil
	}
	return nil, constants.ERR_VAL_NOT_FOUND
}
