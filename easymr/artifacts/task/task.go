package task

import (
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/easymr/constants"
	"time"
)

type taskType int

const (
	SHORT taskType = iota
	LONG
	ROUTINE
	PERMANENT
	ERROR
)

type taskPriority int

const (
	BASE taskPriority = iota
	LOW
	MEDIUM
	HIGH
	URGENT
)

type TaskType interface {
	GetType() taskType
	GetTimeout() time.Time
}

type TaskPriority interface {
	GetPriority() taskPriority
}

func (t *taskType) GetType() taskType {
	return *t
}

// if return nil, this taks is identified as an routine task
func (t *taskType) GetTimeout() time.Duration {
	switch t.GetType() {
	case SHORT:
		return constants.DEFAULT_PERIOD_SHORT
	case LONG:
		return constants.DEFAULT_PERIOD_LONG
	case PERMANENT:
		return constants.DEFAULT_PERIOD_PERMANENT
	default:
		return constants.DEFAULT_PERIOD_PERMANENT
	}
}

func (t *taskPriority) GetPriority() taskPriority {
	return *t
}

func NewCollection() *Collection {
	return &Collection{}
}

func (cg *Collection) Append(cs ...interface{}) *Collection {
	*cg = append(*cg, cs...)
	return cg
}

func (cg *Collection) IsEmpty() bool {
	return len(*cg) == 0
}

func (cg *Collection) Length() int {
	return len(*cg)
}

func (cg *Collection) Get(index int) (interface{}, error) {
	if !cg.IsEmpty() {
		return (*cg)[index], nil
	} else {
		return nil, errors.New(fmt.Sprintf("索引为%d的值是空\n", index))
	}
}
func (cg *Collection) Cap() int {
	return cap(*cg)
}

func (cg *Collection) Filter(f func(interface{}) bool) *Collection {
	var (
		clct = Collection{}
	)

	for _, c := range *cg {
		if f(c) {
			clct = append(clct, c)
		}
	}

	*cg = clct

	return cg
}

type ToStringHandler func() string
type Task struct {
	Type       taskType
	Priority   taskPriority
	Consumable string
	Source     Collection
	Object     interface{}
	Result     Collection
	Context    *TaskContext
	Stage      int
	Local      bool
	Message    string
	//任务运行方式
	RunType int
	TimeOut int
}

func NewErrorTask(msg string) *Task {
	t := &Task{}
	t.Type = ERROR
	t.Message = msg
	return t
}
func (t *Task) GetSourceItem() interface{} {
	if !t.Source.IsEmpty() {
		item, _ := t.Source.Get(0)
		return item
	} else {
		return nil
	}
}

type TaskInfo struct {
	Task  *Task
	Index int
}

type Collection []interface{}

type TaskResult struct {
	Content  interface{}
	Success  bool
	Message  string
	Context  map[string]string
	Finished bool
	Params   map[string][]byte
}

func NewTaskResult(content interface{}) *TaskResult {
	return &TaskResult{content, true, "", make(map[string]string), false, make(map[string][]byte)}
}
func NewEmptyTask() *Task {
	t := &Task{}
	t.Context = &TaskContext{}
	t.RunType = constants.EMPTY_TASK_TYPE
	return t
}
