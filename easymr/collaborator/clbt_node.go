package collaborator

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/iworkable"
	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/collaborator/services"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/server"
)

type ClbtMasterNode struct {
	master       *proxy.NodeProxy
	currentNodes []uint64
}

func newClbtMasterNode() *ClbtMasterNode {
	n := &ClbtMasterNode{}
	n.master = server.GetNodeProxy()
	cards := config.GetCase().GetCardList()
	n.currentNodes = make([]uint64, 0, len(cards))
	for _, c := range cards {
		n.currentNodes = append(n.currentNodes, c.ID)
	}
	return n
}

func (n *ClbtMasterNode) broadcast(todoTasks map[int]*task.Task, localhandler LocalHandler) (map[int]*task.Task, error) {
	var toList []uint64
	var err error
	var all map[int]*task.Task
	var resultTask map[int]*task.Task
	all = make(map[int]*task.Task)
	toList = make([]uint64, 0, len(todoTasks))
	var batchData [][]byte
	var payload []byte

	batchData = make([][]byte, 0, len(todoTasks))
	for i, t := range todoTasks {
		to := uint64(i % len(n.currentNodes))
		taskPair := make(map[int]*task.Task)
		taskPair[i] = t
		if !n.master.IsAlive(to) {
			resultTask, err = localhandler(taskPair)
			all = n.mergeTask(resultTask, all)
			continue
		}
		toList = append(toList, to)
		payload, err = services.Encode(&taskPair, task.SOURCE_SERIALIZE)
		if err != nil {
			payload = nil
			logger.Error("序列化失败:", err)
		}
		batchData = append(batchData, payload)
	}
	batchData, err = n.master.SenMultiMsg(toList, config.MSG_MR_CONSUME, batchData)
	if err != nil {
		return all, err
	}
	n.fillTaskPairs(batchData, all)
	return all, err
}

func (n *ClbtMasterNode) fillTaskPairs(batchData [][]byte, all map[int]*task.Task) {
	for _, payload := range batchData {
		resultTask, err := services.Decode(payload, task.RESULT_SERIALIZE)
		if err != nil {
			logger.Error("填充任务发生错误:", err)
			return
		}
		for i, k := range *resultTask {
			all[i] = k
		}
	}
}

func (n *ClbtMasterNode) mergeTask(merged map[int]*task.Task, all map[int]*task.Task) map[int]*task.Task {
	for k, v := range merged {
		all[k] = v
	}
	return all
}

type ClbtSlaveNode struct {
	slave    *server.Node
	workable iworkable.Workable
}

func newClbtSlaveNode(workable iworkable.Workable) *ClbtSlaveNode {
	n := &ClbtSlaveNode{}
	n.slave = server.GetNode()
	n.slave.RegisterHandler(n)
	n.workable = workable
	return n
}

func (n *ClbtSlaveNode) Handle(msgType uint32, data []byte) ([]byte, bool, error) {
	var maps *map[int]*task.Task
	var result map[int]*task.Task
	var resultBuf []byte
	var err error
	switch msgType {
	case config.MSG_MR_CONSUME:
		maps, err = services.Decode(data, task.SOURCE_SERIALIZE)
		if err != nil {
			return nil, true, err
		}
		result, err = n.doMR(*maps)
		if err != nil {
			return nil, true, err
		}
		resultBuf, err = services.Encode(&result, task.RESULT_SERIALIZE)
		if err != nil {
			return nil, true, err
		}
		return resultBuf, true, nil
	}
	return nil, false, nil
}
func (n *ClbtSlaveNode) doMR(req map[int]*task.Task) (map[int]*task.Task, error) {
	var err error
	err = n.workable.DoneMulti(req)
	return req, err
}
