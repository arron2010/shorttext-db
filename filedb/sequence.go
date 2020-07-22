package filedb

import (
	"encoding/binary"
	"github.com/xp/shorttext-db/glogger"
	"sync/atomic"
)

var logger = glogger.MustGetLogger("filedb")

const seq = "seq"

type SequenceSvc interface {
	Next(name string) uint64
	SetStart(name string, start uint64) error
}

type Sequence struct {
	Start    uint64
	filePath string
	next     uint64
	//key      []byte
	bucket []byte
}

func NewSequence(start uint64) *Sequence {
	s := &Sequence{}
	s.filePath = "/opt/sequence/seq.db"
	s.Start = start
	//s.key = []byte(seq)
	s.bucket = []byte(seq)
	s.next = start
	err := InitBolt(s.filePath, []string{seq})
	if err != nil {
		panic(err)
	}
	logger.Info("完成序列数据库初始化, 数据库名称为", seq)
	return s
}

func (s *Sequence) SetStart(name string, start uint64) error {
	var bytes []byte
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, start)
	err := Put(s.bucket, []byte(name), bytes)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sequence) Next(name string) uint64 {
	//key := "sequence"
	var bytes []byte
	var next uint64
	key := []byte(name)
	bytes = Get(s.bucket, key)
	if len(bytes) == 0 {
		next = 0
	} else {
		next = binary.LittleEndian.Uint64(bytes)
	}

	s.next = atomic.AddUint64(&next, 1)
	bytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, s.next)
	err := Put(s.bucket, key, bytes)
	if err != nil {
		return 0
	}
	return s.next
}

func (s *Sequence) Close() {
	Close()
}
