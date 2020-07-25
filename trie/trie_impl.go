package trie

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"io"
	"sort"
	"strings"
)

var (
	SkipSubtree  = errors.New("忽略该子树")
	ErrNilPrefix = errors.New("前缀对象为空")
)

const (
	DEFAULT_MAX_PREFIX_PERNODE          = 2
	DEFAULT_MAX_CHILDREN_PER_SPARSENODE = 64
)

type NodeItem struct {
	Key    string
	Weight int
}

type (
	Item        []string
	Prefix      []rune
	VisitorFunc func(prefix Prefix, item Item) error
)
type DataItem struct {
	Label string
	ID    string
}

type Trie struct {
	prefix                   Prefix
	item                     Item
	maxPrefixPerNode         int
	maxChildrenPerSparseNode int
	children                 childList
	//foundCache map[string]*Trie
}

func NewTrie() *Trie {
	trie := &Trie{}
	trie.maxPrefixPerNode = DEFAULT_MAX_PREFIX_PERNODE
	trie.maxChildrenPerSparseNode = DEFAULT_MAX_CHILDREN_PER_SPARSENODE
	trie.children = newSparseChildList(trie.maxChildrenPerSparseNode)
	return trie
}

//克隆前缀树，但item是共享的,它被新的Trie引用过来
func (trie *Trie) Clone() *Trie {
	return &Trie{
		prefix:                   append(Prefix(nil), trie.prefix...),
		item:                     trie.item,
		maxPrefixPerNode:         trie.maxPrefixPerNode,
		maxChildrenPerSparseNode: trie.maxChildrenPerSparseNode,
		children:                 trie.children.clone(),
	}
}

func (trie *Trie) Item() Item {
	return trie.item
}

func (trie *Trie) Insert(key Prefix, item Item) (inserted bool) {
	return trie.put(key, item, false)
}

func (trie *Trie) Set(key Prefix, item Item) {
	trie.put(key, item, true)
}

//func (trie *Trie)Append(key Prefix, item Item){
//	trie.put(key, item, false)
//}

func (trie *Trie) Find(key Prefix) (item Item, result bool) {
	var (
		found    bool
		leftover Prefix
		node     *Trie
	)

	//node, found= trie.foundCache[string(key)]
	//if !found{
	//	_, node, found, leftover = trie.findSubtree(key)
	//	if found && len(leftover) == 0 &&  !util.IsNil(node.item ){
	//		trie.foundCache[string(key)]=node
	//		return node.item,true
	//	}
	//	return nil,false
	//}else{
	//	return node.item,true
	//}

	_, node, found, leftover = trie.findSubtree(key)

	if !found || len(leftover) != 0 {
		return nil, false
	}
	if node.item == nil {
		return nil, false
	}
	return node.item, true
}

func (trie *Trie) MatchSubtree(key Prefix) (matched bool) {
	_, _, matched, _ = trie.findSubtree(key)
	return
}

func (trie *Trie) FindAll(visitor VisitorFunc) error {
	return trie.walk(nil, visitor)
}

func (trie *Trie) Size() int {
	n := 0
	trie.walk(nil, func(prefix Prefix, item Item) error {
		n++
		return nil
	})

	return n
}

func (trie *Trie) total() int {
	return 1 + trie.children.total()
}

/*
找出包含word的数据项
*/
func (trie *Trie) FindItems(word string, length int) config.TextSet {
	prefix := Prefix(word)
	result := make(map[string]int)
	trie.VisitSubtree(prefix, func(prefix Prefix, item Item) error {
		for _, v := range item {
			result[v] = length
		}
		return nil
	})
	return result
}

func (trie *Trie) VisitSubtree(prefix Prefix, visitor VisitorFunc) error {
	if prefix == nil {
		panic(ErrNilPrefix)
	}

	if trie.prefix == nil {
		return nil
	}

	_, root, found, leftover := trie.findSubtree(prefix)
	if !found {
		return nil
	}
	prefix = append(prefix, leftover...)

	return root.walk(prefix, visitor)
}

func (trie *Trie) FindPrefixes(key Prefix, visitor VisitorFunc) error {
	if key == nil {
		panic(ErrNilPrefix)
	}

	if trie.prefix == nil {
		return nil
	}

	node := trie
	prefix := key
	offset := 0
	for {
		common := node.longestCommonPrefixLength(key)
		key = key[common:]
		offset += common

		if common < len(node.prefix) {
			return nil
		}

		if item := node.item; item != nil {
			if err := visitor(prefix[:offset], item); err != nil {
				return err
			}
		}

		if len(key) == 0 {
			return nil
		}

		child := node.children.next(key[0])
		if child == nil {
			return nil
		}

		node = child
	}
}

func (trie *Trie) Delete(key Prefix) (deleted bool) {
	if key == nil {
		panic(ErrNilPrefix)
	}

	if trie.prefix == nil {
		return false
	}

	path, found, _ := trie.findSubtreePath(key)
	if !found {
		return false
	}

	node := path[len(path)-1]
	var parent *Trie
	if len(path) != 1 {
		parent = path[len(path)-2]
	}

	if node.item == nil {
		return false
	}

	node.item = nil

	i := len(path) - 1

	if node.children.length() != 0 {
		goto Compact
	}

	if parent == nil {
		node.reset()
		return true
	}

	for ; i >= 0; i-- {
		if current := path[i]; current.item != nil || current.children.length() >= 2 {
			break
		}
	}

	if i == -1 {
		path[0].reset()
		return true
	}

	node = path[i]
	if i == 0 {
		parent = nil
	} else {
		parent = path[i-1]
	}

	node.children.remove(path[i+1].prefix[0])

Compact:
	if compacted := node.compact(); compacted != node {
		if parent == nil {
			*node = *compacted
		} else {
			parent.children.replace(node.prefix[0], compacted)
			*parent = *parent.compact()
		}
	}

	return true
}

func (trie *Trie) DeleteSubtree(prefix Prefix) (deleted bool) {
	if prefix == nil {
		panic(ErrNilPrefix)
	}

	if trie.prefix == nil {
		return false
	}

	parent, root, found, _ := trie.findSubtree(prefix)
	if !found {
		return false
	}

	if parent == nil {
		root.reset()
		return true
	}

	parent.children.remove(root.prefix[0])
	return true
}

func (trie *Trie) empty() bool {
	return trie.item == nil && trie.children.length() == 0
}

func (trie *Trie) reset() {
	trie.prefix = nil
	trie.children = newSparseChildList(trie.maxPrefixPerNode)
}

func (trie *Trie) put(key Prefix, item Item, replace bool) (inserted bool) {
	if key == nil {
		panic(ErrNilPrefix)
	}

	var (
		common int
		node   *Trie = trie
		child  *Trie
	)

	if node.prefix == nil {
		if len(key) <= trie.maxPrefixPerNode {
			node.prefix = key
			goto InsertItem
		}
		node.prefix = key[:trie.maxPrefixPerNode]
		key = key[trie.maxPrefixPerNode:]
		goto AppendChild
	}

	for {
		common = node.longestCommonPrefixLength(key)
		key = key[common:]

		if common < len(node.prefix) {
			goto SplitPrefix
		}

		if len(key) == 0 {
			goto InsertItem
		}

		child = node.children.next(key[0])
		if child == nil {
			goto AppendChild
		}
		node = child
	}

SplitPrefix:
	child = new(Trie)
	*child = *node
	*node = *NewTrie()
	node.prefix = child.prefix[:common]
	child.prefix = child.prefix[common:]
	child = child.compact()
	node.children = node.children.add(child)

AppendChild:
	for len(key) != 0 {
		child := NewTrie()
		if len(key) <= trie.maxPrefixPerNode {
			child.prefix = key
			node.children = node.children.add(child)
			node = child
			goto InsertItem
		} else {
			child.prefix = key[:trie.maxPrefixPerNode]
			key = key[trie.maxPrefixPerNode:]
			node.children = node.children.add(child)
			node = child
		}
	}

InsertItem:

	if replace || node.item == nil {
		node.item = item
		return true
	}

	return false
}

func (trie *Trie) Append(key Prefix, item string) (inserted bool) {
	if key == nil {
		panic(ErrNilPrefix)
	}

	var (
		common int
		node   *Trie = trie
		child  *Trie
	)

	if node.prefix == nil {
		if len(key) <= trie.maxPrefixPerNode {
			node.prefix = key
			goto InsertItem
		}
		node.prefix = key[:trie.maxPrefixPerNode]
		key = key[trie.maxPrefixPerNode:]
		goto AppendChild
	}

	for {
		common = node.longestCommonPrefixLength(key)
		key = key[common:]

		if common < len(node.prefix) {
			goto SplitPrefix
		}

		if len(key) == 0 {
			goto InsertItem
		}

		child = node.children.next(key[0])
		if child == nil {
			goto AppendChild
		}
		node = child
	}

SplitPrefix:
	child = new(Trie)
	*child = *node
	*node = *NewTrie()
	node.prefix = child.prefix[:common]
	child.prefix = child.prefix[common:]
	child = child.compact()
	node.children = node.children.add(child)

AppendChild:
	for len(key) != 0 {
		child := NewTrie()
		if len(key) <= trie.maxPrefixPerNode {
			child.prefix = key
			node.children = node.children.add(child)
			node = child
			goto InsertItem
		} else {
			child.prefix = key[:trie.maxPrefixPerNode]
			key = key[trie.maxPrefixPerNode:]
			node.children = node.children.add(child)
			node = child
		}
	}

InsertItem:
	if node.item == nil {
		node.item = make([]string, 0, 4)
	}
	node.item = append(node.item, item)
	return true
}

func (trie *Trie) compact() *Trie {
	if trie.children.length() != 1 {
		return trie
	}

	child := trie.children.head()

	if trie.item != nil || child.item != nil {
		return trie
	}

	if len(trie.prefix)+len(child.prefix) > trie.maxPrefixPerNode {
		return trie
	}

	child.prefix = append(trie.prefix, child.prefix...)
	if trie.item != nil {
		child.item = trie.item
	}

	return child
}

func (trie *Trie) findSubtree(prefix Prefix) (parent *Trie, root *Trie, found bool, leftover Prefix) {
	root = trie
	for {
		common := root.longestCommonPrefixLength(prefix)
		prefix = prefix[common:]

		if len(prefix) == 0 {
			found = true
			leftover = root.prefix[common:]
			return
		}

		if common < len(root.prefix) {
			leftover = root.prefix[common:]
			return
		}

		child := root.children.next(prefix[0])
		if child == nil {
			return
		}

		parent = root
		root = child
	}
}

func (trie *Trie) findSubtreePath(prefix Prefix) (path []*Trie, found bool, leftover Prefix) {
	root := trie
	var subtreePath []*Trie
	for {
		subtreePath = append(subtreePath, root)

		common := root.longestCommonPrefixLength(prefix)
		prefix = prefix[common:]

		if len(prefix) == 0 {
			path = subtreePath
			found = true
			leftover = root.prefix[common:]
			return
		}

		if common < len(root.prefix) {
			leftover = root.prefix[common:]
			return
		}

		child := root.children.next(prefix[0])
		if child == nil {
			return
		}

		root = child
	}
}

func (trie *Trie) walk(actualRootPrefix Prefix, visitor VisitorFunc) error {
	var prefix Prefix
	if actualRootPrefix == nil {
		prefix = make(Prefix, 32+len(trie.prefix))
		copy(prefix, trie.prefix)
		prefix = prefix[:len(trie.prefix)]
	} else {
		prefix = make(Prefix, 32+len(actualRootPrefix))
		copy(prefix, actualRootPrefix)
		prefix = prefix[:len(actualRootPrefix)]
	}

	if trie.item != nil {
		if err := visitor(prefix, trie.item); err != nil {
			if err == SkipSubtree {
				return nil
			}
			return err
		}
	}

	return trie.children.walk(&prefix, visitor)
}

func (trie *Trie) longestCommonPrefixLength(prefix Prefix) (i int) {
	for ; i < len(prefix) && i < len(trie.prefix) && prefix[i] == trie.prefix[i]; i++ {
	}
	return
}

func (trie *Trie) Dump() string {
	writer := &bytes.Buffer{}
	trie.print(writer, 0)
	return writer.String()
}

func (trie *Trie) print(writer io.Writer, indent int) {
	fmt.Fprintf(writer, "%s%s %v\n", strings.Repeat(" ", indent), string(trie.prefix), trie.item)
	trie.children.print(writer, indent+2)
}

type childList interface {
	length() int
	head() *Trie
	add(child *Trie) childList
	remove(b rune)
	replace(b rune, child *Trie)
	next(b rune) *Trie
	walk(prefix *Prefix, visitor VisitorFunc) error
	print(w io.Writer, indent int)
	clone() childList
	total() int
	all() []*Trie
}

type tries []*Trie

func (t tries) Len() int {
	return len(t)
}

func (t tries) Less(i, j int) bool {
	strings := sort.StringSlice{string(t[i].prefix), string(t[j].prefix)}
	return strings.Less(0, 1)
}

func (t tries) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type sparseChildList struct {
	children tries
}

func newSparseChildList(maxChildrenPerSparseNode int) childList {
	return &sparseChildList{
		children: make(tries, 0, maxChildrenPerSparseNode),
	}
}

func (list *sparseChildList) length() int {
	return len(list.children)
}

func (list *sparseChildList) head() *Trie {
	return list.children[0]
}
func (list *sparseChildList) all() []*Trie {
	return list.children
}

func (list *sparseChildList) add(child *Trie) childList {
	if len(list.children) != cap(list.children) {
		list.children = append(list.children, child)
		return list
	}

	return newDenseChildList(list, child)
}

func (list *sparseChildList) remove(b rune) {
	for i, node := range list.children {
		if node.prefix[0] == b {
			list.children[i] = list.children[len(list.children)-1]
			list.children[len(list.children)-1] = nil
			list.children = list.children[:len(list.children)-1]
			return
		}
	}

	panic("removing non-existent child")
}

func (list *sparseChildList) replace(b rune, child *Trie) {
	if p0 := child.prefix[0]; p0 != b {
		panic(fmt.Errorf("child prefix mismatch: %v != %v", p0, b))
	}

	for i, node := range list.children {
		if node.prefix[0] == b {
			list.children[i] = child
			return
		}
	}
}

func (list *sparseChildList) next(b rune) *Trie {
	for _, child := range list.children {
		if child.prefix[0] == b {
			return child
		}
	}
	return nil
}

func (list *sparseChildList) walk(prefix *Prefix, visitor VisitorFunc) error {

	//sort.Sort(list.children)

	for _, child := range list.children {
		*prefix = append(*prefix, child.prefix...)
		if child.item != nil {
			err := visitor(*prefix, child.item)
			if err != nil {
				if err == SkipSubtree {
					*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
					continue
				}
				*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
				return err
			}
		}

		err := child.children.walk(prefix, visitor)
		*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
		if err != nil {
			return err
		}
	}

	return nil
}

func (list *sparseChildList) total() int {
	tot := 0
	for _, child := range list.children {
		if child != nil {
			tot = tot + child.total()
		}
	}
	return tot
}

func (list *sparseChildList) clone() childList {
	clones := make(tries, len(list.children), cap(list.children))
	for i, child := range list.children {
		clones[i] = child.Clone()
	}

	return &sparseChildList{
		children: clones,
	}
}

func (list *sparseChildList) print(w io.Writer, indent int) {
	for _, child := range list.children {
		if child != nil {
			child.print(w, indent)
		}
	}
}

type denseChildList struct {
	min         int
	max         int
	numChildren int
	headIndex   int
	children    []*Trie
}

func newDenseChildList(list *sparseChildList, child *Trie) childList {
	var (
		min int = 255
		max int = 0
	)
	for _, child := range list.children {
		b := int(child.prefix[0])
		if b < min {
			min = b
		}
		if b > max {
			max = b
		}
	}

	b := int(child.prefix[0])
	if b < min {
		min = b
	}
	if b > max {
		max = b
	}

	children := make([]*Trie, max-min+1)
	for _, child := range list.children {
		children[int(child.prefix[0])-min] = child
	}
	children[int(child.prefix[0])-min] = child

	return &denseChildList{
		min:         min,
		max:         max,
		numChildren: list.length() + 1,
		headIndex:   0,
		children:    children,
	}
}

func (list *denseChildList) length() int {
	return list.numChildren
}

func (list *denseChildList) head() *Trie {
	return list.children[list.headIndex]
}

func (list *denseChildList) all() []*Trie {
	return list.children
}

func (list *denseChildList) add(child *Trie) childList {
	b := int(child.prefix[0])
	var i int

	switch {
	case list.min <= b && b <= list.max:
		if list.children[b-list.min] != nil {
			panic("dense child list collision detected")
		}
		i = b - list.min
		list.children[i] = child

	case b < list.min:
		children := make([]*Trie, list.max-b+1)
		i = 0
		children[i] = child
		copy(children[list.min-b:], list.children)
		list.children = children
		list.min = b

	default: // b > list.max
		children := make([]*Trie, b-list.min+1)
		i = b - list.min
		children[i] = child
		copy(children, list.children)
		list.children = children
		list.max = b
	}

	list.numChildren++
	if i < list.headIndex {
		list.headIndex = i
	}
	return list
}

func (list *denseChildList) remove(b rune) {
	i := int(b) - list.min
	if list.children[i] == nil {
		panic("removing non-existent child")
	}
	list.numChildren--
	list.children[i] = nil

	if i == list.headIndex {
		for ; i < len(list.children); i++ {
			if list.children[i] != nil {
				list.headIndex = i
				return
			}
		}
	}
}

func (list *denseChildList) replace(b rune, child *Trie) {
	if p0 := child.prefix[0]; p0 != b {
		panic(fmt.Errorf("child prefix mismatch: %v != %v", p0, b))
	}

	list.children[int(b)-list.min] = child
}

func (list *denseChildList) next(b rune) *Trie {
	i := int(b)
	if i < list.min || list.max < i {
		return nil
	}
	return list.children[i-list.min]
}

func (list *denseChildList) walk(prefix *Prefix, visitor VisitorFunc) error {
	for _, child := range list.children {
		if child == nil {
			continue
		}
		*prefix = append(*prefix, child.prefix...)
		if child.item != nil {
			if err := visitor(*prefix, child.item); err != nil {
				if err == SkipSubtree {
					*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
					continue
				}
				*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
				return err
			}
		}

		err := child.children.walk(prefix, visitor)
		*prefix = (*prefix)[:len(*prefix)-len(child.prefix)]
		if err != nil {
			return err
		}
	}

	return nil
}

func (list *denseChildList) print(w io.Writer, indent int) {
	for _, child := range list.children {
		if child != nil {
			child.print(w, indent)
		}
	}
}

func (list *denseChildList) clone() childList {
	clones := make(tries, cap(list.children))

	if list.numChildren != 0 {
		clonedCount := 0
		for i := list.headIndex; i < len(list.children); i++ {
			child := list.children[i]
			if child != nil {
				clones[i] = child.Clone()
				clonedCount++
				if clonedCount == list.numChildren {
					break
				}
			}
		}
	}

	return &denseChildList{
		min:         list.min,
		max:         list.max,
		numChildren: list.numChildren,
		headIndex:   list.headIndex,
		children:    clones,
	}
}

func (list *denseChildList) total() int {
	tot := 0
	for _, child := range list.children {
		if child != nil {
			tot = tot + child.total()
		}
	}
	return tot
}
