package services

import (
	. "github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/store"
)

func Encode(maps *map[int]*Task, mode int) (payload []byte, err error) {

	payload, err = store.GetInstance().MessageEncoder.Encode(maps, mode)

	return payload, err

}

func Decode(payload []byte, mode int) (maps *map[int]*Task, err error) {

	maps, err = store.GetInstance().MessageEncoder.Decode(payload, mode)

	return maps, err
}
