package memkv

import (
	"github.com/tidwall/match"
	"github.com/tidwall/rtree"

	"strings"
)

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	btr     *BTree                                 // contains the items
	rtr     *rtree.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b *DBItem) bool                // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *DB                                    // the origin database
	opts    IndexOptions                           // index options
}

// match matches the pattern to the key
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index) clearCopy() *index {
	// copy the index meta information
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = New(btreeDegrees, nidx)
	}
	if nidx.rect != nil {
		nidx.rtr = rtree.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index
func (idx *index) rebuild() {
	// initialize trees
	if idx.less != nil {
		idx.btr = New(btreeDegrees, idx)
	}
	if idx.rect != nil {
		idx.rtr = rtree.New(idx)
	}
	// iterate through all keys and fill the index
	idx.db.keys.Ascend(func(item *DBItem) bool {
		dbi := item
		//if !idx.match(dbi.key) {
		//	// does not match the pattern, continue
		//	return true
		//}
		if idx.less != nil {
			idx.btr.ReplaceOrInsert(dbi)
		}
		//if idx.rect != nil {
		//	idx.rtr.Insert(dbi)
		//}
		return true
	})
}
