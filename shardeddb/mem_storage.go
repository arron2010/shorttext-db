package shardeddb

import (
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/gjson"
	"github.com/xp/shorttext-db/memdb"
	"github.com/xp/shorttext-db/utils"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type IMemStorage interface {
	Get(key string) (string, error)
	Set(key string, text string) error
	SetWithIndex(key string, text string, prefixName string) error
	Find(text string) ([]entities.Record, error)
	Delete(key string) error
	Save() error
	Open() error
	Close() error
	GetKeyCount() int
}

//对内存数据库的封装,提供简易接口
type memStorage struct {
	name  string
	db    *memdb.DB
	path  string
	index Index
	count int
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

	go m.persistent()
	return m, nil
}

func (m *memStorage) persistent() {
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
	if err != nil {
		m.increaseCount(1)
	}

	return err
}

func (m *memStorage) SetWithIndex(key string, text string, prefixName string) error {
	var err error
	//if !gjson.Valid(text){
	//	return errors.New(fmt.Sprintf("文本[%s]不符合Json格式",text))
	//}
	err = m.db.Update(func(tx *memdb.Tx) error {
		_, _, err := tx.Set(key, text, nil)
		if err == nil {
			desc := gjson.Get(text, prefixName).Str
			if len(desc) == 0 {
				return errors.New(fmt.Sprintf("主键:%s ,字段:%s, 错误信息:创建索引时字段为空", key, prefixName))
			}
			if !utils.IsNil(m.index) {
				err = m.index.Create(desc, key)
			}
		}
		return err
	})
	if err == nil {
		m.increaseCount(1)
	}

	return err
}

func (m *memStorage) Delete(key string) error {
	err := m.db.Update(func(tx *memdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
	if err != nil {
		m.increaseCount(-1)
	}
	return err
}

/*
查找文本命中的记录
*/
func (m *memStorage) Find(text string) ([]entities.Record, error) {
	var r entities.Record
	result := make([]entities.Record, 0)
	keyWords, err := m.index.Parse(text)
	if err != nil {
		return result, err
	}
	kwLen := m.lengthWords(keyWords)
	found, err := m.index.Find(keyWords, kwLen)
	if err != nil {
		return result, err
	}
	for k, v := range found {
		text, err := m.Get(k)
		if err != nil {
			return result, err
		}
		if len(text) == 0 {
			continue
		}
		r = m.createRecord(text, v)
		result = append(result, r)
	}
	return result, err
}

/*
根据gjson字符格式，创建记录对象
*/
func (m *memStorage) createRecord(text string, ratio float32) entities.Record {
	var r entities.Record = entities.Record{}
	r.PrefixRatio = ratio
	r.Desc = gjson.Get(text, config.GJSON_FIELD_DESC).Str
	r.Id = gjson.Get(text, config.GJSON_FIELD_ID).Str
	return r
}

/*
获得分词后，文本的总长度
*/
func (m *memStorage) lengthWords(words []config.Text) int {
	var l int
	for _, v := range words {
		l = l + len(v)
	}
	return l
}

/*
键计数器，每次增加一条记录加一,删除数据减一
*/
func (m *memStorage) increaseCount(delta int64) {
	var nCount int
	key := "key_count"
	strCount, err := m.Get(key)
	if err != nil && err.Error() != "not found" {
		logger.Errorf("Service:memStorage,Message:获取Key数量报错|%s\n", err.Error())
		return
	}
	if len(strCount) == 0 {
		nCount = 0
	} else {
		nCount, err = strconv.Atoi(strCount)
		if err != nil {
			logger.Errorf("Service:memStorage,Message:类型转换报错|%s|%s\n", strCount, err.Error())
			return
		}
	}

	nCount64 := int64(nCount)
	m.count = int(atomic.AddInt64(&nCount64, delta))
	m.Set(key, strconv.Itoa(m.count))

}

/*
获得本库键的总数
*/
func (m *memStorage) GetKeyCount() int {
	return m.count
}
