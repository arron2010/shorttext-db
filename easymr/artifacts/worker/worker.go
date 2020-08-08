package worker

import (
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/store"
)

//var logger = glogger.MustGetLogger("worker")

type Worker struct {
	ID          uint
	Alive       bool
	BaseTasks   chan *task.TaskFuture
	LowTasks    chan *task.TaskFuture
	MediumTasks chan *task.TaskFuture
	HighTasks   chan *task.TaskFuture
	UrgentTasks chan *task.TaskFuture
	Exit        chan bool
}

func (w *Worker) Start() {
	fs := store.GetInstance()
	go func() {
		for {
			select {
			case <-w.Exit:
				return
			default:
				tkf := preselect(
					w.UrgentTasks,
					w.HighTasks,
					w.MediumTasks,
					w.LowTasks,
					w.BaseTasks,
				)
				tk := tkf.Receive()
				//logger.Info(fmt.Sprintf(
				//	"Worker%v:, Task Level:%v",
				//	w.ID,
				//	tk.Priority,
				//))
				//	tk.Context.Context[constants.WORKER_ID] = w.ID
				tkf.Return(fs.CallEx(
					(*tk).Consumable,
					w.ID,
					tk,
				))
			}
		}
	}()
}

func (w *Worker) GetID() uint {
	return w.ID
}

func (w *Worker) Quit() {
	w.Exit <- true
}

func preselect(a, b, c, d, e chan *task.TaskFuture) *task.TaskFuture {
	select {
	case x := <-a:
		return x
	default:
	}

	select {
	case x := <-a:
		return x
	case x := <-b:
		return x
	default:
	}

	select {
	case x := <-a:
		return x
	case x := <-b:
		return x
	case x := <-c:
		return x
	default:
	}

	select {
	case x := <-a:
		return x
	case x := <-b:
		return x
	case x := <-c:
		return x
	case x := <-d:
		return x
	default:
	}

	select {
	case x := <-a:
		return x
	case x := <-b:
		return x
	case x := <-c:
		return x
	case x := <-d:
		return x
	case x := <-e:
		return x
	}
}
