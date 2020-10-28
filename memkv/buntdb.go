package memkv

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/xp/shorttext-db/memkv/proto"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xp/shorttext-db/btree"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

// DB represents a collection of key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields
	file      *os.File          // the underlying file
	buf       []byte            // a buffer to write to
	keys      *btree.BTree      // a tree of all item ordered by key
	exps      *btree.BTree      // a tree of items ordered by expiration
	idxs      map[string]*index // the index trees.
	exmgr     bool              // indicates that expires manager is running.
	flushes   int               // a count of the number of disk flushes
	closed    bool              // set when the database has been closed
	config    Config            // the database configuration
	persist   bool              // do we write to disk
	shrinking bool              // when an aof shrink is in-process.
	lastaofsz int               // the size of the last shrink aof size
	id        uint32            //数据库标识

}
type DBItem struct {
	Key         Key
	RawKey      Key
	Val         Value
	ValueType   uint32
	StartTS     uint64
	CommitTS    uint64
	Op          uint32
	Ttl         uint64
	ForUpdateTS uint64
	TxnSize     uint64
	MinCommitTS uint64
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	OnExpired func(keys []Key)

	// OnExpiredSync will be called inside the same transaction that is performing
	// the deletion of expired items. If OnExpired is present then this callback
	// will not be called. If this callback is present, then the deletion of the
	// timeed-out item is the explicit responsibility of this callback.
	OnExpiredSync func(key Key, value Value, tx *Tx) error
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *DB
}

// Default number of btree degrees
const btreeDegrees = 32

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.keys = btree.New(btreeDegrees, nil)
	db.exps = btree.New(btreeDegrees, &exctx{db})
	db.idxs = make(map[string]*index)
	// initialize default configuration
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	if db.persist {
		db.file.Sync() // do a sync but ignore the error
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

// Save writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases using the ":memory:". Database that persist to disk
// can be snapshotted by simply copying the database file.
func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	// use a buffered writer and flush every 4MB
	var buf []byte
	// iterated through every item in the database and write to the buffer
	db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*DBItem)
		buf = dbi.writeSetTo(buf)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final flush
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
func (db *DB) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persist {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	return db.readLoad(rd, time.Now())
}
func (db *DB) LoadDB() error {
	return nil
}

func (db *DB) PersistDB() error {
	return nil
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b *DBItem) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// If a previous index with the same name exists, that index will be deleted.
func (db *DB) ReplaceIndex(name, pattern string,
	less ...func(a, b *DBItem) bool) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index.
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *DBItem) *DBItem {
	var pdbi *DBItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*DBItem)
		//if pdbi.opts != nil && pdbi.opts.ex {
		//	// Remove it from the exipres tree.
		//	db.exps.Delete(pdbi)
		//}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			//if idx.rtr != nil {
			//	// Remove it from the rtree index.
			//	idx.rtr.Remove(pdbi)
			//}
		}
	}
	//if item.opts != nil && item.opts.ex {
	//	// The new item has eviction options. Add it to the
	//	// expires tree
	//	db.exps.ReplaceOrInsert(item)
	//}
	for _, idx := range db.idxs {
		//if !idx.match(item.key) {
		//	continue
		//}
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.ReplaceOrInsert(item)
		}
		//if idx.rtr != nil {
		//	// Add new item to rtree index.
		//	idx.rtr.Insert(item)
		//}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&DBItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *DBItem) *DBItem {
	var pdbi *DBItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*DBItem)
		//if pdbi.opts != nil && pdbi.opts.ex {
		//	// Remove it from the exipres tree.
		//	db.exps.Delete(pdbi)
		//}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			//if idx.rtr != nil {
			//	// Remove it from the rtree index.
			//	idx.rtr.Remove(pdbi)
			//}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		var onExpired func([]Key)
		var expired []*DBItem
		var onExpiredSync func(key Key, value Value, tx *Tx) error
		err := db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			// produce a list of expired items that need removing
			//db.exps.AscendLessThan(&DBItem{
			//	opts: &dbItemOpts{ex: true, exat: time.Now()},
			//}, func(item btree.Item) bool {
			//	expired = append(expired, item.(*DBItem))
			//	return true
			//})
			if onExpired == nil && onExpiredSync == nil {
				for _, itm := range expired {
					if _, err := tx.Delete(itm.Key); err != nil {
						// it's ok to get a "not found" because the
						// 'Delete' method reports "not found" for
						// expired items.
						if err != ErrNotFound {
							return err
						}
					}
				}
			} else if onExpiredSync != nil {
				for _, itm := range expired {
					if err := onExpiredSync(itm.Key, itm.Val, tx); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		if onExpired != nil && len(expired) > 0 {
			keys := make([]Key, 0, 32)
			for _, itm := range expired {
				keys = append(keys, itm.Key)
			}
			onExpired(keys)
		}

		// execute a disk sync, if needed
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			if db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				_ = db.file.Sync()
				flushes = db.flushes
			}
		}()
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.file.Name()
	tmpname := fname + ".tmp"
	// the endpos is used to return to the end of the file when we are
	// finished writing all of the current items.
	endpos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmpname)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpname)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := []byte("")
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			db.keys.AscendGreaterOrEqual(&DBItem{Key: pivot},
				func(item btree.Item) bool {
					dbi := item.(*DBItem)
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.Key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here is really bad. So just panic.
		if err := os.Rename(tmpname, fname); err != nil {
			panic(err)
		}
		db.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

var errValidEOF = errors.New("valid eof")

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader, should be no greater than
// the current time.Now().
func (db *DB) readLoad(rd io.Reader, modTime time.Time) error {
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// read a single command.
		// first we should read the number of parts that the of the command
		line, err := r.ReadBytes('\n')
		if err != nil {
			if len(line) > 0 {
				// got an eof but also data. this should be an unexpected eof.
				return io.ErrUnexpectedEOF
			}
			if err == io.EOF {
				break
			}
			return err
		}
		if line[0] != '*' {
			return ErrInvalid
		}
		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return err
			}
			if line[0] != '$' {
				return ErrInvalid
			}
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return ErrInvalid
			}
			db.insertIntoDatabase(&DBItem{Key: []byte(parts[1]), Val: []byte(parts[2])})

			//if len(parts) == 5 {
			//	if strings.ToLower(parts[3]) != "ex" {
			//		return ErrInvalid
			//	}
			//	ex, err := strconv.ParseInt(parts[4], 10, 64)
			//	if err != nil {
			//		return err
			//	}
			//	now := time.Now()
			//	dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
			//	if dur > 0 {
			//		db.insertIntoDatabase(&DBItem{
			//			Key: []byte(parts[1]),
			//			val: []byte(parts[2]),
			//			opts: &dbItemOpts{
			//				ex:   true,
			//				exat: now.Add(dur),
			//			},
			//		})
			//	}
			//} else {
			//	db.insertIntoDatabase(&DBItem{Key: []byte(parts[1]), val: []byte(parts[2])})
			//}
		} else if (parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return ErrInvalid
			}
			db.deleteFromDatabase(&DBItem{Key: []byte(parts[1])})
		} else if (parts[0][0] == 'f' || parts[0][0] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			db.keys = btree.New(btreeDegrees, nil)
			db.exps = btree.New(btreeDegrees, &exctx{db})
			db.idxs = make(map[string]*index)
		} else {
			return ErrInvalid
		}
	}
	return nil
}

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	if err := db.readLoad(db.file, fi.ModTime()); err != nil {
		return err
	}
	pos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.lastaofsz = int(pos)
	return nil
}
func (db *DB) Put(item *DBItem) (err error) {
	tx := &Tx{
		db: db, writable: true,
	}
	_, _, err = tx.Set(item)
	return err
}
func (db *DB) Get(key Key) (val *DBItem) {
	val = &DBItem{}
	tx := &Tx{
		db: db, writable: true,
	}
	item := tx.db.get(key)
	if item != nil {
		val = item
	}
	return val
}
func (db *DB) Delete(key Key) (err error) {
	tx := &Tx{
		db: db, writable: true,
	}
	_, err = tx.Delete(key)
	return err
}

const lockVer uint64 = math.MaxUint64

func (db *DB) SetId(id uint32) {
	db.id = id
}

func (db *DB) Find(key Key) *proto.DBItems {
	result := make([]*proto.DBItem, 0)
	stop := mvccEncode(key, 0)
	start := mvccEncode(key, lockVer)
	tx := &Tx{
		db: db, writable: true,
	}
	tx.scanKeys(start, stop, func(dbi *DBItem) bool {
		result = append(result, &proto.DBItem{Key: dbi.Key, Value: dbi.Val})
		return true
	})
	return &proto.DBItems{Items: result}
}

func (db *DB) Ascend(index string,
	iterator func(key Key, value *DBItem) bool) error {
	return db.View(func(tx *Tx) error {
		return tx.scan(false, false, false, index, nil, nil, iterator)
	})
}

func (db *DB) GetByRange(start Key, stop Key) []*DBItem {
	result := make([]*DBItem, 0)
	db.managed(true, func(tx *Tx) error {
		tx.scanKeys(start, stop, func(dbi *DBItem) bool {
			result = append(result, dbi)
			return true
		})
		return nil
	})
	return result
}

func (db *DB) RecordCount() int {
	return 0
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			//fmt.Println("memdb error:",err)
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key Key) *DBItem {
	item := db.keys.Get(&DBItem{Key: key})
	if item != nil {
		return item.(*DBItem)
	}
	return nil
}

// DeleteAll deletes all items from the database.
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	if tx.wc.rbkeys == nil {
		// we need to backup the live data in case of a rollback.
		tx.wc.rbkeys = tx.db.keys
		tx.wc.rbexps = tx.db.exps
		tx.wc.rbidxs = tx.db.idxs
	}

	// now reset the live database trees
	tx.db.keys = btree.New(btreeDegrees, nil)
	tx.db.exps = btree.New(btreeDegrees, &exctx{tx.db})
	tx.db.idxs = make(map[string]*index)

	// finally re-create the indexes
	for name, idx := range tx.wc.rbidxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.commitItems = make(map[string]*DBItem)

	return nil
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	return tx, nil
}
func (db *DB) AscendGreaterOrEqual(index string, pivot *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return db.View(func(tx *Tx) error {
		return tx.AscendGreaterOrEqual(index, pivot, iterator)
	})
}

func (db *DB) AscendRange(index string, greaterOrEqual, lessThan *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return db.View(func(tx *Tx) error {
		return tx.AscendRange(index, greaterOrEqual, lessThan, iterator)
	})
}

func (db *DB) DescendRange(index string, greaterOrEqual, lessThan *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return db.View(func(tx *Tx) error {
		return tx.DescendRange(index, greaterOrEqual, lessThan, iterator)
	})
}

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}
func appendBulkBytes(buf []byte, s []byte) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *DBItem) writeSetTo(buf []byte) []byte {
	//if dbi.opts != nil && dbi.opts.ex {
	//	ex := dbi.opts.exat.Sub(time.Now()) / time.Second
	//	buf = appendArray(buf, 5)
	//	buf = appendBulkString(buf, "set")
	//	buf = appendBulkBytes(buf, dbi.key)
	//	buf = appendBulkBytes(buf, dbi.val)
	//	buf = appendBulkString(buf, "ex")
	//	buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
	//} else {
	//	buf = appendArray(buf, 3)
	//	buf = appendBulkString(buf, "set")
	//	buf = appendBulkBytes(buf, dbi.Key)
	//	buf = appendBulkBytes(buf, dbi.val)
	//}

	buf = appendArray(buf, 3)
	buf = appendBulkString(buf, "set")
	buf = appendBulkBytes(buf, dbi.Key)
	buf = appendBulkBytes(buf, dbi.Val)
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *DBItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkBytes(buf, dbi.Key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *DBItem) expired() bool {
	return false
	//return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *DBItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*DBItem)

	//if dbi.sortKey !=nil && dbi2.sortKey !=nil{
	//	return bytes.Compare(dbi.sortKey, dbi2.sortKey) < 0
	//	//return false//dbi.sortKey < dbi2.sortKey
	//}

	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		//if dbi2.expiresAt().After(dbi.expiresAt()) {
		//	return true
		//}
		//if dbi.expiresAt().After(dbi2.expiresAt()) {
		//	return false
		//}
	case *index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi, dbi2) {
				return true
			}
			if ctx.less(dbi2, dbi) {
				return false
			}
		}
	}

	// Always fall back to the Key comparison. This creates absolute uniqueness.
	//if dbi.keyless {
	//	return false
	//} else if dbi2.keyless {
	//	return true
	//}
	return bytes.Compare(dbi.Key, dbi2.Key) < 0
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() Key-value will expire
	Expires bool
	// TTL is how much time the Key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.

func IndexKey(a, b *DBItem) bool {
	result := bytes.Compare(a.Key, b.Key)
	if result < 0 {
		return true
	}
	return false
}
func IndexRawKey(a, b *DBItem) bool {
	result := bytes.Compare(a.RawKey, b.RawKey)
	if result < 0 {
		return true
	}
	return false
}

func IndexRawKeyLen(a, b *DBItem) bool {
	if len(a.RawKey) < len(b.RawKey) {
		return true
	}
	return false
}

func IndexStartTS(a, b *DBItem) bool {
	if a.StartTS < b.StartTS {
		return true
	}
	return false
}

func IndexCommitTS(a, b *DBItem) bool {
	if a.CommitTS < b.CommitTS {
		return true
	}
	return false
}

func IndexRawKeyAndCommitTS(a, b *DBItem) bool {
	result := bytes.Compare(a.RawKey, b.RawKey)
	if result < 0 && a.CommitTS < b.CommitTS {
		return true
	}
	return false
}
