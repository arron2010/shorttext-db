package shardeddb

import (
	"github.com/xp/shorttext-db/gjson"
	"github.com/xp/shorttext-db/memdb"
	"github.com/xp/shorttext-db/utils"
	"os"
	"strconv"
	"time"
)

type IMemStorage interface {
	Get(key string) (string, error)
	Set(key string, text string) error
	SetWithPrefix(key string, text string, prefixName string) error
	//Find(prefix string)([]string,error)
	GetIndex() Index
	Delete(key string) error
	Save() error
	Open() error
	Close() error
}

//对内存数据库的封装,提供简易接口
type memStorage struct {
	name  string
	db    *memdb.DB
	path  string
	index Index
}

func newMemStorage(id int, path string, name string) (*memStorage, error) {
	m := &memStorage{}
	m.name = name
	m.path = path + "/" + strconv.Itoa(id) + "/" + m.name + ".db"
	m.name = name
	err := m.Open()
	if err != nil {
		return nil, err
	}
	m.index = NewIndex()

	go m.autoPersistent()
	return m, nil
}

func (m *memStorage) autoPersistent() {
	heartbeat := time.NewTicker(time.Second * 3000)
	defer heartbeat.Stop()
	for range heartbeat.C {
		m.Save()
	}
}
func (m *memStorage) Open() error {
	var err error
	var fs *os.File
	m.db, err = memdb.Open(":memory:")
	if !utils.IsExist(m.path) {
		return nil
	}
	fs, err = os.Open(m.path)
	if err != nil {
		return err
	}
	err = m.db.Load(fs)
	return err
}

func (m *memStorage) Close() error {
	return m.db.Close()
}

func (m *memStorage) Save() error {
	var (
		err  error
		file *os.File
	)
	file, err = os.Create(m.path)
	defer file.Close()
	if err != nil {
		return err
	}
	err = m.db.Save(file)
	if err != nil {
		return err
	}
	return err
}

func (m *memStorage) Get(key string) (string, error) {

	var err error
	var text string
	err = m.db.View(func(tx *memdb.Tx) error {
		text, err = tx.Get(key)
		return err
	})

	return text, err
}

func (m *memStorage) Set(key string, text string) error {
	var err error

	err = m.db.Update(func(tx *memdb.Tx) error {
		_, _, err := tx.Set(key, text, nil)
		return err
	})
	return err
}

func (m *memStorage) SetWithPrefix(key string, text string, prefixName string) error {
	var err error
	//if !gjson.Valid(text){
	//	return errors.New(fmt.Sprintf("文本[%s]不符合Json格式",text))
	//}
	err = m.db.Update(func(tx *memdb.Tx) error {
		_, _, err := tx.Set(key, text, nil)
		if err != nil {
			desc := gjson.Get(text, prefixName).Str
			if !utils.IsNil(m.index) {
				err = m.index.Create(desc, key)
			}
		}
		return err
	})
	return err
}

func (m *memStorage) Delete(key string) error {
	err := m.db.Update(func(tx *memdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
	return err
}

func (m *memStorage) GetIndex() Index {
	return m.index
}
