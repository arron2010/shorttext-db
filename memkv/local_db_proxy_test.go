package memkv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestLocalDBProxy_Scan(t *testing.T) {
	local := NewLocalDBProxy(1)
	var item *DBItem
	item = &DBItem{RawKey: []byte("B5"), StartTS: 5, CommitTS: 80}
	local.Put(item)
	item = &DBItem{RawKey: []byte("B2"), StartTS: 5, CommitTS: 70}
	local.Put(item)
	item = &DBItem{RawKey: []byte("B2"), StartTS: 6, CommitTS: 40}
	local.Put(item)
	item = &DBItem{RawKey: []byte("A4"), StartTS: 7, CommitTS: 70}
	local.Put(item)
	item = &DBItem{RawKey: []byte("A5"), StartTS: 4, CommitTS: 40}
	local.Put(item)

	//var db2 *DB
	//db2 = local.db.(*DB)
	//db2.View(func(tx *Tx) error {
	//	tx.Ascend("RawKeyAndCommitTSIndex", func(key Key, value *DBItem) bool {
	//		fmt.Println(*value)
	//		return true
	//	})
	//	return nil
	//})
	r := local.Scan(Key("A1"), Key("B5"), 50, 100, false, nil)
	for _, v := range r {
		fmt.Println(*v)
	}
	//fmt.Println(r)
	//result,_ := local.GetByRawKey(Key("B2"),50)
	//fmt.Println(*result)

}

func TestLocalDBProxy_Scan3(t *testing.T) {
	local := NewLocalDBProxy(2)
	var item *DBItem

	item = &DBItem{RawKey: []byte{1, 2, 3, 4, 5, 6, 7}, StartTS: 7, CommitTS: 70}
	local.Put(item)

	item = &DBItem{RawKey: []byte{1, 2, 3, 4, 5}, StartTS: 5, CommitTS: 70}
	local.Put(item)
	item = &DBItem{RawKey: []byte{1, 2, 3, 4, 5, 6}, StartTS: 6, CommitTS: 40}
	local.Put(item)

	item = &DBItem{RawKey: []byte{1, 2, 3, 4, 5, 6, 7, 8}, StartTS: 4, CommitTS: 40}
	local.Put(item)

	item = &DBItem{RawKey: []byte{1, 2, 3, 4}, StartTS: 5, CommitTS: 80}
	local.Put(item)

	//var db2 *DB
	//db2 = local.db.(*DB)
	//db2.View(func(tx *Tx) error {
	//	tx.Ascend("IndexRawKey", func(key Key, value *DBItem) bool {
	//		fmt.Println(*value)
	//		return true
	//	})
	//	return nil
	//})
	//r := local.Scan(Key("A1"),Key("B5"),50,100,false,nil)
	//for _,v := range r{
	//	fmt.Println(*v)
	//}
	//fmt.Println(r)
	result, _ := local.GetByRawKey([]byte{1, 2, 3, 4}, 100)
	fmt.Println(*result)

}

func test01(a []int) {
	for i := 1; i <= 32; i++ {
		a = append(a, i)
	}
}
func TestLocalDBProxy_Scan2(t *testing.T) {
	a := []byte{1, 2, 3, 4, 5}
	b := []byte{2}
	fmt.Println(bytes.Compare(a, b))
}
