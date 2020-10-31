package memkv

import (
	"bytes"
	proto2 "github.com/golang/protobuf/proto"
)

type Op int32

var Op_name = map[int32]string{
	0: "Put",
	1: "Del",
	2: "Lock",
	3: "Rollback",
	4: "Insert",
	5: "PessimisticLock",
	6: "CheckNotExists",
}
var Op_value = map[string]int32{
	"Put":             0,
	"Del":             1,
	"Lock":            2,
	"Rollback":        3,
	"Insert":          4,
	"PessimisticLock": 5,
	"CheckNotExists":  6,
}

func (x Op) String() string {
	return proto2.EnumName(Op_name, int32(x))
}

type LocalDBProxy struct {
	db        MemDB
	sequence  uint64
	readCount uint64
}

func NewLocalDBProxy(id uint32) *LocalDBProxy {
	var err error
	l := &LocalDBProxy{}
	l.db, err = Open(":memory:")
	//l.db.CreateIndex("RawKeyAndCommitTSIndex", "*", IndexRawKey,IndexCommitTS)
	l.db.CreateIndex("KeyIndex", "*", IndexKey)
	l.db.CreateIndex("IndexRawKey", "*", IndexRawKey)

	if err != nil {
		panic(err)
	}
	l.db.SetId(id)

	return l
}

func (l *LocalDBProxy) Close() error {
	return l.db.Close()
}

func (l *LocalDBProxy) GetByRawKey(key []byte, ts uint64) (result *DBItem, validated bool) {
	pivot := &DBItem{RawKey: key, CommitTS: ts}
	result = &DBItem{}
	l.db.AscendGreaterOrEqual("IndexRawKey", pivot, func(key Key, value *DBItem) bool {
		//fmt.Println(string(value.RawKey))
		if pivot.CommitTS >= value.CommitTS && bytes.Compare(pivot.RawKey, value.RawKey) == 0 {
			result = value
			return false
		}
		return true
	})
	//允许值为空数据，插入到数据库。因此不能使用len(result.Val) != 0判断值是否有效
	return result, len(result.RawKey) != 0
}
func (l *LocalDBProxy) Put(item *DBItem) (err error) {

	//xhelper.Print("LocalDBProxy-->Put","RawKey-->",item.RawKey," Value-->",len(item.Val)," CommitTS-->",item.CommitTS,"OP-->",Op(item.Op).String())
	//debug.PrintStack()
	db := l.db
	item.Key = mvccEncode(item.RawKey, item.CommitTS)
	err = db.Put(item)
	return err
}
func (l *LocalDBProxy) Get(key []byte, ts uint64) (item *DBItem, validated bool) {
	k := mvccEncode(key, ts)
	item = l.db.Get(k)
	return item, len(item.RawKey) != 0
}
func (l *LocalDBProxy) FindByKey(finding Key, locked bool) []*DBItem {
	result := make([]*DBItem, 0, 4)
	l.db.Ascend("IndexRawKey", func(key Key, value *DBItem) bool {
		if bytes.Compare(finding, value.RawKey) == 0 {
			result = append(result, value)
		}
		return true
	})
	return result
}
func (l *LocalDBProxy) Delete(key []byte, ts uint64) (err error) {
	db := l.db
	k := mvccEncode(key, ts)
	//item,ok := l.Get(key,ts)
	//if ok{
	//	xhelper.Print("LocalDBProxy-->Delete 92","RawKey-->",item.RawKey," Value-->",len(item.Val)," ts-->",ts,"OP-->",Op(item.Op).String())
	//}else{
	//	xhelper.Print("LocalDBProxy-->Delete 94","RawKey-->",key,"ts-->",ts)
	//}
	return db.Delete(k)
}

func iterator(value *DBItem, ts uint64, limit int, validate ValidateFunc, result []*DBItem) (bool, []*DBItem) {
	if len(result) == limit {
		return false, result
	}
	if validate != nil && !validate(value) {
		return false, result
	}
	if ts > value.CommitTS {
		result = append(result, value)
	}
	return true, result
}
func (l *LocalDBProxy) Scan(startKey Key, endKey Key, ts uint64, limit int, desc bool, validate ValidateFunc) []*DBItem {
	result := make([]*DBItem, 0, 0)
	if !desc {
		l.db.AscendRange("IndexRawKey",
			&DBItem{RawKey: startKey, CommitTS: ts},
			&DBItem{RawKey: endKey, CommitTS: ts},
			func(key Key, value *DBItem) bool {
				var flag bool
				flag, result = iterator(value, ts, limit, validate, result)
				return flag
			},
		)
	} else {
		l.db.DescendRange("IndexRawKey",
			&DBItem{RawKey: startKey, CommitTS: ts},
			&DBItem{RawKey: endKey, CommitTS: ts},
			func(key Key, value *DBItem) bool {
				var flag bool
				flag, result = iterator(value, ts, limit, validate, result)
				return flag
			},
		)
	}

	return result
}

func (l *LocalDBProxy) generateId() uint64 {
	//id := atomic.AddUint64(&l.sequence, 1)
	l.sequence = l.sequence + 1
	return l.sequence
}
