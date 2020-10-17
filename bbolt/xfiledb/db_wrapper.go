package xfiledb

import (
	"github.com/xp/shorttext-db/bbolt"
	"github.com/xp/shorttext-db/glogger"
	"os"
	"time"
)

var logger = glogger.MustGetLogger("xfiledb")
var fileMode os.FileMode = 0600 // owner can read and writ
type DBWrapper struct {
	name   string
	bucket []byte
	db     *bbolt.DB
}
type DBPair struct {
	Key   []byte
	Value []byte
}

func NewDB(name string) *DBWrapper {
	db := &DBWrapper{name: name, bucket: []byte(name)}
	return db
}
func (w *DBWrapper) Open() error {
	database := "/opt/xfiledb/" + w.name + ".db"
	var err error
	w.db, err = bbolt.Open(database, fileMode, &bbolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return err
	}
	err = w.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(w.bucket))
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// Get value from bucket by key
func (w *DBWrapper) Get(key []byte) []byte {
	var value []byte
	err := w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(w.bucket)
		v := b.Get(key)
		if v != nil {
			value = append(value, b.Get(key)...)
		}
		return nil
	})
	if err != nil {
		logger.Errorf("数据库:%s Key:%v 获取数据失败\n", w.name, key)
		return nil
	}
	return value
}

// Put a key/value pair into target bucket
func (w *DBWrapper) Put(key []byte, value []byte) error {
	err := w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(w.bucket)
		err := b.Put(key, value)
		return err
	})
	return err
}

func (w *DBWrapper) Delete(key []byte) error {
	err := w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(w.bucket)
		err := b.Delete(key)
		return err
	})

	return err
}

func (w *DBWrapper) GetAllKeyValues() []DBPair {
	var pairs []DBPair

	w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(w.bucket)
		b.ForEach(func(k, v []byte) error {
			// Due to
			// Byte slices return ed from Bolt are only valid during a transaction.
			//Once the transaction has been committed or
			//rolled back then the memory they point to can be reused by a new page or can be unmapped from virtual memory
			//and you'll see an unexpected fault address panic when accessing it.
			// We copy the slice to retain it
			dstk := make([]byte, len(k))
			dstv := make([]byte, len(v))
			copy(dstk, k)
			copy(dstv, v)

			pair := DBPair{dstk, dstv}
			pairs = append(pairs, pair)
			return nil
		})
		return nil
	})

	return pairs
}

func (w *DBWrapper) Close() error {
	return w.db.Close()
}
