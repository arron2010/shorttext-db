package services

import (
	. "github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/store"
)

func Encode(maps *map[int]*Task, mode int) (payload *TaskPayload, err error) {
	//var maps_bytes bytes.Buffer
	//
	//enc := gob.NewEncoder(&maps_bytes)
	//
	//err = enc.Encode(maps)
	//
	//payload = &TaskPayload{
	//	Payload: maps_bytes.Bytes(),
	//}
	//return

	payload, err = store.GetInstance().MessageEncoder.Encode(maps, mode)

	return payload, err

}

func Decode(payload *TaskPayload, mode int) (maps *map[int]*Task, err error) {
	//dec := gob.NewDecoder(bytes.NewReader(payload.GetPayload()))
	//err = dec.Decode(&maps)
	maps, err = store.GetInstance().MessageEncoder.Decode(payload.GetPayload(), mode)
	//将二进制流直接赋值给task
	//for _,v := range *maps{
	//	v.BinaryContent=payload.BigPayload
	//}

	return maps, err
}
