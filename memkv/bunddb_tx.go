package memkv

import (
	"github.com/xp/shorttext-db/btree"
	"sort"
	"strings"
	"time"
)

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	// rollback when deleteAll is called
	rbkeys *btree.BTree      // a tree of all item ordered by key
	rbexps *btree.BTree      // a tree of items ordered by expiration
	rbidxs map[string]*index // the index trees.

	rollbackItems   map[string]*DBItem // details for rolling back tx.
	commitItems     map[string]*DBItem // details for committing tx.
	itercount       int                // stack of iterators
	rollbackIndexes map[string]*index  // details for dropped indexes.
}

func (tx *Tx) Print() {
}

// Indexes returns a list of index names.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// DropIndex removes an index.
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(tx.db.idxs, name)
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate that the
			// index should be rebuilt upon rollback.
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (tx *Tx) createIndex(name string, pattern string, lessers []func(a, b *DBItem) bool, opts *IndexOptions) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := tx.db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b *DBItem) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b *DBItem) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	tx.db.idxs[name] = idx
	//if tx.wc.rbkeys == nil {
	//	// store the index in the rollback map.
	//	if _, ok := tx.wc.rollbackIndexes[name]; !ok {
	//		// we use nil to indicate that the index should be removed upon rollback.
	//		tx.wc.rollbackIndexes[name] = nil
	//	}
	//}
	return nil
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b *DBItem) bool) error {
	return tx.createIndex(name, pattern, less, opts)
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
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b *DBItem) bool) error {
	return tx.createIndex(name, pattern, less, nil)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(index string, pivot *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	var err error
	var less func(a, b *DBItem) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key Key, value *DBItem) bool {
		if less(pivot, value) {
			return false
		}
		return iterator(key, value)
	})
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index string, pivot *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(false, true, false, index, pivot, nil, iterator)
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
func (tx *Tx) GetLess(index string) (func(a, b *DBItem) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index string, start, stop *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*DBItem)
		return iterator(dbi.Key, dbi)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *DBItem

	itemA = start
	itemB = stop

	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error

	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbkeys != nil {
		tx.db.keys = tx.wc.rbkeys
		tx.db.idxs = tx.wc.rbidxs
		tx.db.exps = tx.wc.rbexps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&DBItem{Key: []byte(key)})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.idxs, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			tx.db.idxs[name] = idx
			idx.rebuild()
		}
	}
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	//if tx.writable {
	//	tx.rollbackInner()
	//}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
func (tx *Tx) TTL(key Key) (time.Duration, error) {

	return 0, nil
}

// Set inserts or replaces an item in the database based on the Key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Set(item *DBItem) (previousValue Value,
	replaced bool, err error) {
	if tx.db == nil {
		return nil, false, ErrTxClosed
	} else if !tx.writable {
		return nil, false, ErrTxNotWritable
	}

	prev := tx.db.insertIntoDatabase(item)

	if prev != nil {
		previousValue, replaced = prev.Val, true
	}

	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// the found value will be returned even if it is expired.
func (tx *Tx) Get(key Key, ignoreExpired ...bool) (val Value, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := tx.db.get(key)
	if item == nil || (item.expired() && !ignore) {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return nil, ErrNotFound
	}
	return item.Val, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key Key) (val Value, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if !tx.writable {
		return nil, ErrTxNotWritable
	}

	//else if tx.wc.itercount > 0 {
	//	return nil, ErrTxIterating
	//}
	item := tx.db.deleteFromDatabase(&DBItem{Key: key})
	if item == nil {
		return nil, ErrNotFound
	}
	return item.Val, nil
}

func (tx *Tx) scanKeys(start Key, stop Key,
	iterator func(*DBItem) bool) error {
	var tr *btree.BTree
	tr = tx.db.keys
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*DBItem)
		return iterator(dbi)
	}
	var itemA, itemB *DBItem
	itemA = &DBItem{Key: start}
	itemB = &DBItem{Key: stop}
	if len(stop) == 0 {
		tr.AscendGreaterOrEqual(itemA, iter)
	} else {
		tr.AscendRange(itemA, itemB, iter)
	}

	return nil
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(false, false, false, index, nil, nil, iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index string, pivot *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(false, false, true, index, pivot, nil, iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index string, greaterOrEqual, lessThan *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(true, false, false, index, nil, nil, iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index string, pivot *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(true, true, false, index, pivot, nil, iterator)
}

func (tx *Tx) DescendRange(index string, lessOrEqual, greaterThan *DBItem,
	iterator func(key Key, value *DBItem) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}
