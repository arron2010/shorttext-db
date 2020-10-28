package memkv

import (
	"github.com/xp/shorttext-db/memkv/proto"
)

type ListIterator struct {
	data    []*proto.DBItem
	cursor  int
	descend bool
}

func NewEmptytIterator() *ListIterator {
	return NewListIterator(emptyItems, false)
}

func NewListIterator(data *proto.DBItems, descend bool) *ListIterator {
	if data == nil {
		data = &proto.DBItems{}
	}
	iter := &ListIterator{data: data.Items}
	if len(data.Items) == 0 {
		iter.cursor = -1
	}

	iter.data = data.Items
	if !descend {
		if len(iter.data) > 0 {
			iter.cursor = 0
		}
	} else {
		if len(iter.data) > 0 {
			iter.cursor = len(iter.data) - 1
		}
	}
	return iter
}

func (l *ListIterator) Next() {
	l.cursor = l.cursor + 1
}

func (l *ListIterator) Prev() bool {
	l.cursor = l.cursor - 1
	return l.Valid()
}

func (l *ListIterator) Valid() bool {
	if l.cursor < 0 || len(l.data) <= 0 || l.cursor >= len(l.data) {
		return false
	}
	return true
}

func (l *ListIterator) Key() []byte {
	if l.cursor > -1 && l.cursor < len(l.data) {
		return l.data[l.cursor].Key
	}
	return nil
}

func (l *ListIterator) Value() []byte {
	if l.cursor > -1 && l.cursor < len(l.data) {
		return l.data[l.cursor].Value
	}
	return nil
}

//type memdbIterator struct {
//	dbi       *DBItem
//	validated bool
//	cursor    *btree.Cursor
//}
//
//func (m *memdbIterator) Next() {
//	item := m.cursor.Next()
//	if item == nil || utils.IsNil(item) {
//		m.dbi = nil
//		m.validated = false
//		return
//	}
//	m.dbi = item.(*DBItem)
//}
//func (m *memdbIterator) Valid() bool {
//	return m.validated
//}
//
//func (m *memdbIterator) Key() []byte {
//	if m.dbi != nil {
//		return m.dbi.key
//	} else {
//		return nil
//	}
//}
//
//func (m *memdbIterator) Value() []byte {
//	if m.dbi != nil {
//		return m.dbi.val
//	} else {
//		return nil
//	}
//}
//
////	Key()
