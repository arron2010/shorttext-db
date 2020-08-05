package taskHelper

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/glogger"
)

var logger = glogger.MustGetLogger("taskHelper")
var MAX_TASK_COUNT = constants.DEFAULT_MAX_TASK_COUNT

// slice the data source of the map into N separate segments
func Slice(inmaps map[int]*task.Task, n int) map[int]*task.Task {
	//if n < 2 {
	//	return inmaps
	//}
	//
	//var (
	//	//gap     = len(inmaps)
	//	gap     = len(inmaps)
	//	outmaps = make(map[int]*task.Task)
	//)
	//for k, t := range inmaps {
	//	var (
	//		sgap = len(t.Source)
	//		i    = 0
	//	)
	//	if n > sgap {
	//		n = sgap
	//	}
	//	var outGap int = gap
	//	if gap < sgap {
	//		outGap = sgap
	//	}
	//	for ; i < n-1; i++ {
	//
	//		source := t.Source[i*getGap(sgap, n) : (i+1)*getGap(sgap, n)]
	//		//xiaopeng modifeid on 2018-12-30
	//		//有多个任务加入的时候，(k+1)*gap+i会出现重复，导致任务被覆盖
	//		outKey := (k+1)*outGap + i
	//		outmaps[outKey] = &task.Task{t.Type, t.Priority, t.Consumable, source, t.Result, t.Context, t.Stage, false, []byte{}, 0, 0}
	//	}
	//	outKey := (k+1)*outGap + i
	//	source := t.Source[i*getGap(sgap, n):]
	//	outmaps[outKey] = &task.Task{t.Type, t.Priority, t.Consumable, source, t.Result, t.Context, t.Stage, false, []byte{}, 0, 0}
	//}

	return nil
}

func calculateSliceNum(t *task.Task, c int) int {
	srcLen := t.Source.Length()
	if srcLen > 50 && srcLen < 100 {
		return 10
	}
	if srcLen > 100 {
		return c
	}
	return 3
}

func AutoSlice(inmaps map[int]*task.Task) map[int]*task.Task {
	//strMaxCount := MAX_TASK_COUNT
	//var (
	//	//gap     = len(inmaps)
	//	gap     = len(inmaps)
	//	outmaps = make(map[int]*task.Task)
	//)
	//for k, t := range inmaps {
	//	var (
	//		sgap = len(t.Source)
	//		i    = 0
	//	)
	//	n := calculateSliceNum(t, strMaxCount)
	//	if n == 0 {
	//		outmaps[k] = t
	//	}
	//
	//	if n > sgap {
	//		n = sgap
	//	}
	//
	//	var outGap int = gap
	//	if gap < sgap {
	//		outGap = sgap
	//	}
	//	for ; i < n-1; i++ {
	//
	//		source := t.Source[i*getGap(sgap, n) : (i+1)*getGap(sgap, n)]
	//		//xiaopeng modifeid on 2018-12-30
	//		//有多个任务加入的时候，(k+1)*gap+i会出现重复，导致任务被覆盖
	//		outKey := (k+1)*outGap + i
	//		outmaps[outKey] = &task.Task{t.Type, t.Priority, t.Consumable, source, t.Result, t.Context, t.Stage, false, []byte{}, 0, 0}
	//	}
	//	outKey := (k+1)*outGap + i
	//	source := t.Source[i*getGap(sgap, n):]
	//	outmaps[outKey] = &task.Task{t.Type, t.Priority, t.Consumable, source, t.Result, t.Context, t.Stage, false, []byte{}, 0, 0}
	//}
	//
	//return outmaps
	return nil
}

func getGap(length int, count int) int {
	if count > length {
		return 1
	} else {
		//count =0
		return length / count
	}
}
